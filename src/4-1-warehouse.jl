##
# Code for 4.1: Warehouse Operations Simulation with Queue Management
#
##


"""
Represents a shipment in the warehouse simulation.

Stores information to calculate different statistics
about waiting and processing time after the end of the 
simulation.
"""
mutable struct Shipment
    arrival_time::Float64
    start_processing::Float64
    end_processing::Float64

    """
    Create new shipment with only arrival time set
    """
    Shipment(arrival_time) = new(arrival_time, 0, 0)
end

"""
Represents a worker in the simulation that processes
shipments from the different queues.
"""
mutable struct Worker
    sim::Simulation
    working_time::Float64
    current_shipment::Union{Shipment,Nothing}

    function Worker(sim::Simulation, state, p_g, p_f)
        w = new(sim, 0.0, nothing)
        @process worker_process(sim, state, w, p_g, p_f)
        w
    end
end

"""
Finalize the statistics of a Worker.

Required to correctl count incomplete shipment processing in statistics.
"""
function finalize!(w::Worker)
    if !isnothing(w.current_shipment)
        w.working_time += now(w.sim) - w.current_shipment.start_processing
    end
end

"""
Warehouse queue sued in the simulation to represent
incoming groceries or frozen goods.

    ShipmentQueue(λ, Q_max)

with `λ` being the average arrival rate and `Q_max` the maximum
queue size. 
"""
mutable struct ShipmentQueue
    sim::Simulation
    λ::Float64
    Q_max::UInt64
    full_time::Float64
    full_start_time::Float64
    empty_time::Float64
    empty_start_time::Float64
    shipments::QueueStore{Shipment}
    rejected_shipments::Store{Shipment}

    """
        ShipmentQueue(λ, Q_max)

    Initialize new shipment queue with arrival rate `λ`
    and maximum queue size of `Q_max`.
    """
    ShipmentQueue(sim, λ, Q_max) = new(sim, λ, Q_max, 0, -1, 0, 0,
        QueueStore{Shipment}(sim), Store{Shipment}(sim))
end

"""
    isfull(q::ShipmentQueue) 

Returns true, if a ShipmentQueue is full because it reached `Q_max`
shipments.
"""
isfull(q::ShipmentQueue) = q.shipments.load >= q.Q_max

function Base.iterate(q::ShipmentQueue)
    iterate(q.shipments.items)
end

function Base.iterate(q::ShipmentQueue, state)
    iterate(q.shipments.items, state)
end

"""
    length(q::ShipmentQueue) 
    
Returns the number of shipments currently in the queue.
"""
Base.length(q::ShipmentQueue) = q.shipments.load

"""
    put!(q::ShipmentQueue, s::Shipment)

Put a new shipment to the ShipmentQueue.
Throws an error if the ShipmentQueue is full.
"""
function Base.put!(q::ShipmentQueue, s::Shipment)
    if !isfull(q)
        if isempty(q)
            @debug "$(now(q.sim)) Something in there again"
            q.empty_time += now(q.sim) - q.empty_start_time
            q.empty_start_time = -1.0
        end
        put!(q.shipments, s)
        if isfull(q) && q.full_start_time < 0
            q.full_start_time = now(q.sim)
        end
    else
        if q.full_start_time < 0
            q.full_start_time = now(q.sim)
        end
        put!(q.rejected_shipments, s)
        throw("Can't put $s. ShipmentQueue is full!")
    end
end

"""
    take!(q::ShipmentQueue)

Takes a shipment from the ShipmentQueue.
If not shipment is available, it will block until a new 
shipment is put into the queue.
"""
function Base.take!(q::ShipmentQueue)
    if isfull(q)
        q.full_time += now(q.sim) - q.full_start_time
        @debug "$(now(q.sim)) Not full again $(q.full_time)"
        q.full_start_time = -1.0
    end
    s = take!(q.shipments)
    if isempty(q)
        @debug "$(now(q.sim)) Empty again"
        q.empty_start_time = now(q.sim)
    end
    s
end

"""
    finalize!(q::ShipmentQueue)

Finalize the statistics of a ShipmentQueue.

Required to correctl count incomplete shipment processing in statistics.
"""
function finalize!(q::ShipmentQueue)
    if q.empty_start_time >= 0
        q.empty_time += now(q.sim) - q.empty_start_time
    end
    if q.full_start_time >= 0
        q.full_time += now(q.sim) - q.full_start_time
    end
end

struct WarehouseState
    grocery_queue::ShipmentQueue
    frozen_queue::ShipmentQueue
    processed_groceries::Store{Shipment}
    processed_frozen::Store{Shipment}
    avaliable_shipments::Store{Nothing}

    function WarehouseState(sim, λ_g, λ_f, Q_g, Q_f)
        warehouse = new(ShipmentQueue(sim, λ_g, Q_g), ShipmentQueue(sim, λ_f, Q_f),
            Store{Shipment}(sim), Store{Shipment}(sim), Store{Nothing}(sim))
        @process arrival_process(sim, warehouse, warehouse.grocery_queue)
        @process arrival_process(sim, warehouse, warehouse.frozen_queue)
        warehouse
    end
end

"""
    finalize!(state::WarehouseState)

Finalize the statistics of a WarehouseState.

Required to correctl count incomplete shipment processing in statistics.
"""
function finalize!(state::WarehouseState)
    finalize!(state.grocery_queue)
    finalize!(state.frozen_queue)
end

"""
    arrival_process(sim::Simulation, warehouse_state::WarehouseState,
                    queue::ShipmentQueue)

Simulation process used to simulate shipment arrivals for 
warehouse queues.
"""
@resumable function arrival_process(sim::Simulation, warehouse_state::WarehouseState,
    queue::ShipmentQueue)
    while true
        next_arrival = rand(Exponential(1 / queue.λ))
        @yield timeout(sim, next_arrival)
        new_shipment = Shipment(now(sim))
        try
            @debug "T $(now(sim)): $new_shipment arrived in $queue"
            put!(queue, new_shipment)
            put!(warehouse_state.avaliable_shipments, nothing)
        catch e
            @debug "T $(now(sim)): $new_shipment rejected: $e"
            if typeof(e) == MethodError
                throw(e)
            end
        end
    end
end

"""
    worker_process(sim::Simulation, warehouse_state::WarehouseState,
                    worker::Worker, p_g, p_f)

Simulation process used to simulate worker behavior
"""
@resumable function worker_process(sim::Simulation, warehouse_state::WarehouseState, worker::Worker, p_g, p_f)
    while true
        @debug "T $(now(sim)): $worker waits for items"
        @yield take!(warehouse_state.avaliable_shipments)
        start_time = now(sim)
        @debug "T $(now(sim)): $worker starts processing"
        if (length(warehouse_state.grocery_queue) > length(warehouse_state.frozen_queue))
            worker.current_shipment = @yield take!(warehouse_state.grocery_queue)
            worker.current_shipment.start_processing = now(sim)
            @yield timeout(sim, rand(Exponential(p_g)))
            worker.current_shipment.end_processing = now(sim)
            put!(warehouse_state.processed_groceries, worker.current_shipment)
        else
            worker.current_shipment = @yield take!(warehouse_state.frozen_queue)
            worker.current_shipment.start_processing = now(sim)
            @yield timeout(sim, rand(Exponential(p_f)))
            worker.current_shipment.end_processing = now(sim)
            put!(warehouse_state.processed_frozen, worker.current_shipment)
        end
        worker.working_time += now(sim) - start_time
    end
end

"""

    simulate_warehouse_queue(λ_g, λ_f, p_g,p_f, Q_g, Q_f, n, duration)

Simulate Warehouse queue management based on the
provided configuration for a given duration.

- λ_g: arrival rates for groceries
- λ_f: arrival rate for frozen goods
- p_g: processing times for groceries
- p_f: processing times for frozen goods
- Q_g: queue size for groceries
- Q_f: queue size for frozen goods
- n: number of workers processing the items one at a time
- duration: duration of the simulation

Returns a DataFrame with the used configuration and selected KPIs.

"""
function simulate_warehouse_queue(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)
    sim = Simulation()
    state = WarehouseState(sim, λ_g, λ_f, Q_g, Q_f)
    workers = [Worker(sim, state, p_g, p_f) for _ in 1:n]
    @debug "Start Simulation"
    run(sim, duration)
    finalize!.(workers)
    finalize!(state)
    DataFrame(Dict("λ_g" => [λ_g], "λ_f" => [λ_f], "Q_g" => [Q_g], "Q_f" => [Q_f], "p_g" => [p_g],
        "p_f" => [p_f], "n" => [n], "total_rejects_g" => [state.grocery_queue.rejected_shipments.load],
        "total_rejects_f" => [state.frozen_queue.rejected_shipments.load],
        "total_finished_g" => [state.processed_groceries.load],
        "total_finished_f" => [state.processed_frozen.load], "duration" => [duration],
        "worker_util_rate" => [if !isempty(workers)
            mean(w.working_time for w in workers) / duration
        else
            0
        end],
        "avg_wait_g" => [if !isempty(state.processed_groceries.items)
            mean(s.start_processing - s.arrival_time for (s, _) in state.processed_groceries.items)
        else
            0.0
        end],
        "avg_wait_f" => [if !isempty(state.processed_frozen.items)
            mean(s.start_processing - s.arrival_time for (s, _) in state.processed_frozen.items)
        else
            0.0
        end],
        "full_rate_g" => [state.grocery_queue.full_time / duration],
        "full_rate_f" => [state.frozen_queue.full_time / duration],
        "empty_rate_g" => [state.grocery_queue.empty_time / duration],
        "empty_rate_f" => [state.frozen_queue.empty_time / duration]))
end
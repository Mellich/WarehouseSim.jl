module WarehouseSim

using ConcurrentSim
using DataFrames
using Distributions
using Logging
using ResumableFunctions
using Statistics
using Base: iterate, put!, take!, length

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
    d = iterate(q.shipments.items)
    if !isnothing(d)
        return first(d), last(d)
    end
    d
end

function Base.iterate(q::ShipmentQueue, state)
    d = iterate(q.shipments.items, state)
    if !isnothing(d)
        return first(d), last(d)
    end
    d
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
            @info "$(now(q.sim)) Something in there again"
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
        @info "$(now(q.sim)) Not full again $(q.full_time)"
        q.full_start_time = -1.0
    end
    s = take!(q.shipments)
    if isempty(q)
        @info "$(now(q.sim)) Empty again"
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
            @info "T $(now(sim)): $new_shipment arrived in $queue"
            put!(queue, new_shipment)
            put!(warehouse_state.avaliable_shipments, nothing)
        catch e
            @info "T $(now(sim)): $new_shipment rejected: $e"
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
        @info "T $(now(sim)): $worker waits for items"
        @yield take!(warehouse_state.avaliable_shipments)
        start_time = now(sim)
        @info "T $(now(sim)): $worker starts processing"
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
    @info "Start Simulation"
    run(sim, duration)
    finalize!.(workers)
    finalize!(state)
    DataFrame(Dict("λ_g" => [λ_g], "λ_f" => [λ_f], "Q_g" => [Q_g], "Q_f" => [Q_f], "p_g" => [p_g],
        "p_f" => [p_f], "n" => [n], "rejects_g" => [state.grocery_queue.rejected_shipments.load],
        "rejects_f" => [state.frozen_queue.rejected_shipments.load],
        "finished_g" => [state.processed_groceries.load],
        "finished_f" => [state.processed_frozen.load], "duration" => [duration],
        "worker_util" => [if !isempty(workers)
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

"""
    simulate_parametrized(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)

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

All parameters can also be an `AbstractArray` or a range. In that case, 
all combinations of input parameters will be simulated.
Returns a DataFrame with the used configuration for each combination.

Examples:

Simulate a given configuration for different durations between 1000 and 5000 time units:

    simulate_parametrized(1, 4, 1, 2, 10, 100, 8, 1000:1000:5000)

Simulate a configuration with different λ_g given as array:

    simulate_parametrized([1, 4, 6.3], 4, 1, 2, 10, 100, 8, 1000)

If multiple input parameters :

    simulate_parametrized([1, 2, 3], [1, 2, 3], 1, 2, 10, 100, 8, 1000)

In this case, all combinations for the input parameters will be processed,
resulting in a total of 6 simulation runs.


"""
function simulate_parametrized(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)
    df = DataFrame()
    # Lock to synchronize aggregation of partial results for each simulation run
    df_lock = Threads.Condition()
    # Get all combinations of input parameters
    # and execute simulations over all available threads
    # Use dynamic scheduling to improve load balancing
    Threads.@threads :dynamic for (λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration) in collect(Iterators.product(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration))
        df_part = simulate_warehouse_queue(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)
        Threads.lock(df_lock)
        append!(df, df_part)
        Threads.unlock(df_lock)
    end
    df
end

export simulate_warehouse_queue
export isfull, iterate, length, put!, take!
export Worker, Shipment, ShipmentQueue, WarehouseState
end
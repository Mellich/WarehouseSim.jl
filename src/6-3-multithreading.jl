##
# Code for 6.3: Parallel Processing for Simulation
#
##

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

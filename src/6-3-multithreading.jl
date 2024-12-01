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
    # Create a DataFrame for each thread to reduce synchronization overhead
    df_threads = [DataFrame() for _ in 1:Threads.nthreads()]
    # Get all combinations of input parameters
    # and execute simulations over all available threads
    # Ensure dynamic scheduling to improve load balancing
    Threads.@threads :dynamic for (λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration) in collect(Iterators.product(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration))
        df_part = simulate_warehouse_queue(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)
        append!(df_threads[Threads.threadid()], df_part)
    end
    # Reduce collected data to single DataFrame after all threads have completed
    reduce(vcat, df_threads)
end

"""
    visualize_table(df::DataFrame)

Create a window with a sortable table from a DataFrame.
"""
function visualize_table(df::DataFrame)
    w = Blink.Window()
    body!(w, showtable(df))
end

"""
    visualize_plot(df::DataFrame)

Create a window with scatter plot and selectable columns for the axis.
"""
function visualize_plot(df::DataFrame)
    fig = Figure()

    # Create some menus to select columns for X and Y axis
    x_content = Menu(fig, options=names(df), default=first(names(df)))
    y_content = Menu(fig, options=names(df), default=last(names(df)))
    fig[1, 1] = vgrid!(
        Label(fig, "X-Axis", width=nothing),
        x_content,
        Label(fig, "Y-Axis", width=nothing),
        y_content;
        tellheight=false, width=200)

    # Use Observables to update plot
    x_col = Observable{String}(first(names(df)))
    y_col = Observable{String}(last(names(df)))

    ax = Axis(fig[1, 2], xlabel=x_col, ylabel=y_col)

    # Update x and y values on selection
    x_vals = lift(x_col) do v
        Float64.(df[!, v])
    end
    y_vals = lift(y_col) do v
        Float64.(df[!, v])
    end

    scatter!(ax, x_vals, y_vals)

    # Trigger selection and auto scaling of plot
    on(x_content.selection) do x_c
        x_col[] = x_c
        autolimits!(ax)
    end
    notify(x_content.selection)
    on(y_content.selection) do y_c
        y_col[] = y_c
        autolimits!(ax)
    end
    notify(y_content.selection)
    fig
end

# Warehouse Simulator

This project implements case 1 from group 4 _Warehouse Operations Simulation with Queue Management_ and on top of it group 6 case 3 _Parallel Processing for Simulation_.

It uses `ConcurrentSim` for the implementation of the DES and `DataFrames` for the output of statistical information to simplify post-processing and aggregation of simulation results.

## 4-1: Warehouse Simulation

Core of the simulator is the function `simulate_warehouse_queue(λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)` taking the following input parameters as also specified in the case study:

- `λ_g`: arrival rates for groceries
- `λ_f`: arrival rate for frozen goods
- `p_g`: processing times for groceries
- `p_f`: processing times for frozen goods
- `Q_g`: queue size for groceries
- `Q_f`: queue size for frozen goods
- `n`: number of workers processing the items one at a time
- `duration`: duration of the simulation

To run the simulation, activate this project and run the following lines e.g. in the REPL:

```{julia}
using WarehouseSim

df = simulate_warehouse_queue(1.0, 1.0, 2.0, 0.5, 20, 20, 2, 100)
```

## 6-3: Multi-Threaded execution and Visualization

The multithreaded execution and the visualization of the simulation results is implemented in `6-3-multithreading.jl`.
Since there was freedom in choosing the queueing system, the solution from 4-1 is used.

Parametrized execution of the simulation can be done through `simulate_parametrized((λ_g, λ_f, p_g, p_f, Q_g, Q_f, n, duration)`. Each of the input parameters can also be a range or collection.
The returned DataFrame can be inspected and visualized using the following two options:

- a interactive table view via `visualize_table(df)`
- a 2D scatter plot with selectable axis via `visualize_plot(df)`

Executing parametrized simulation runs using multiple threads and direct visualization can e.g. 
be achieved like this:

```{julia}
using WarehouseSim
visualize_plot(simulate_parametrized([0.5, 4, 2], 4, 1, 2, 10, 100, 8, 100:200))
```

## Example Script

An example script is provided in `example/simulate.jl`.
Execute the simulations with 4 threads and plot results:

    cd example
    julia --project -t 4 -i simulate.jl

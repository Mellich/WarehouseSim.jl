using WarehouseSim

df = simulate_parametrized([0.5, 4, 2], 4, 1, 2, 10, 10:10:100, 1:8, 100:50:200)
display(visualize_plot(df))
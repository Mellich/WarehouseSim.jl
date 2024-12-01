using Test
using WarehouseSim
using DataFrames
using ConcurrentSim

"""
Test basic functionality of Shipment data structure
"""
@testset "Shipment" begin
    s = Shipment(2.0)
    @test s.arrival_time == 2.0
    @test s.start_processing == 0.0
    @test s.end_processing == 0.0
end

"""
Test basic functionality of ShipmentQueue
"""
@testset "ShipmentQueue" begin
    sim = Simulation()
    s = ShipmentQueue(sim, 2.0, 5)
    @test s.λ == 2.0
    @test s.Q_max == 5
    @test_throws InexactError ShipmentQueue(sim, 2.0, 3.5)
    @test is_empty(s)
    @test length(s) == 0
    shipment = Shipment(1)
    put!(s, shipment)
    for i in 2:5
        @test is_full(s) == false
        put!(s, Shipment(i))
        @test is_empty(s) == false
    end
    @test is_full(s)
    @test length(s) == 5
    @test_throws "ShipmentQueue is full!" put!(s, Shipment(6))
    @test length(s) == 5
    @test value(take!(s)) == shipment
    @test length(s) == 4
end

"""
Simple test run of simulation and basic sanity checks
of output data.
"""
@testset "Simulation" begin
    df = simulate_warehouse_queue(1, 1, 1, 1, 10, 10, 2, 20)
    @test typeof(df) == DataFrame
    @test first(df.n) == 2
    @test 0.0 <= first(df.worker_util) <= 1.0
    @test 0.0 <= first(df.full_rate_g) <= 1.0
    @test 0.0 <= first(df.full_rate_f) <= 1.0
    @test 0.0 <= first(df.empty_rate_g) <= 1.0
    @test 0.0 <= first(df.empty_rate_f) <= 1.0
end
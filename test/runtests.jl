using Test
using WarehouseSim
using DataFrames
using ConcurrentSim

@testset "WarehouseSim" begin
    @testset "Test basic functionality of Shipment data structure" begin
        s = Shipment(2.0)
        @test s.arrival_time == 2.0
        @test s.start_processing == 0.0
        @test s.end_processing == 0.0
    end


    @testset "Basic functionality ShipmentQueue" begin
        sim = Simulation()
        s = ShipmentQueue(sim, 2.0, 5)
        @test s.λ == 2.0
        @test s.Q_max == 5
        @test_throws InexactError ShipmentQueue(sim, 2.0, 3.5)
        @test isempty(s)
        @test length(s) == 0
        shipment = Shipment(1)
        put!(s, shipment)
        for i in 2:5
            @test isfull(s) == false
            put!(s, Shipment(i))
            @test isempty(s) == false
        end
        @test isfull(s)
        @test length(s) == 5
        @test_throws "ShipmentQueue is full!" put!(s, Shipment(6))
        @test length(s) == 5
        @test value(take!(s)) == shipment
        @test length(s) == 4
    end

    @testset "Simple test run of simulation and basic sanity checks of output data." begin
        df = simulate_warehouse_queue(1, 2, 3, 4, 10, 20, 2, 30)
        @test typeof(df) == DataFrame
        @test first(df.num_workers) == 2
        @test first(df.λ_f) == 2.0
        @test first(df.λ_g) == 1.0
        @test first(df.p_f) == 4.0
        @test first(df.p_g) == 3.0
        @test first(df.Q_f) == 20
        @test first(df.Q_g) == 10
        @test first(df.duration) == 30
        @test 0.0 <= first(df.worker_util_rate) <= 1.0
        @test 0.0 <= first(df.full_rate_g) <= 1.0
        @test 0.0 <= first(df.full_rate_f) <= 1.0
        @test 0.0 <= first(df.empty_rate_g) <= 1.0
        @test 0.0 <= first(df.empty_rate_f) <= 1.0
    end

    @testset "Nearly no items arrive in grocery queue." begin
        df = simulate_warehouse_queue(0.0001, 1, 1, 1, 10, 10, 1, 0.1)
        @test 0.0 <= first(df.full_rate_g) <= 0.0001
        @test 0.99 <= first(df.empty_rate_g) <= 1.0
    end

    @testset "Nearly no items arrive in frozen queue." begin
        df = simulate_warehouse_queue(0.0001, 0.0001, 1, 1, 10, 10, 1, 0.1)
        @test 0.0 <= first(df.full_rate_f) <= 0.0001
        @test 0.99 <= first(df.empty_rate_f) <= 1.0
    end

    @testset "Overflow grocery queue." begin
        df = simulate_warehouse_queue(100, 0.0001, 100, 1, 10, 10, 1, 100)
        @test 0.9 <= first(df.full_rate_g) <= 1.0
        @test 0.0 <= first(df.empty_rate_g) <= 0.1
        @test first(df.total_rejects_g) > 0
    end

    @testset "Overflow frozen queue." begin
        df = simulate_warehouse_queue(0.0001, 100, 1, 100, 10, 10, 1, 100)
        @test 0.9 <= first(df.full_rate_f) <= 1.0
        @test 0.0 <= first(df.empty_rate_f) <= 0.1
        @test first(df.total_rejects_f) > 0
    end

    @testset "Parametrized Simulation single parameter" begin
        df = simulate_parametrized(1, 1, 4, 6, 10, 10, 1:3, 1)
        @test size(df, 1) == 3
        @test all(n in df[!, "num_workers"] for n in 1:3)
    end

    @testset "Parametrized Simulation multiple parameters" begin
        df = simulate_parametrized(1, 1, 4, 6, 10:10:30, 10, 1:3, 1)
        @test size(df, 1) == 9
        @test all(n in df[!, "num_workers"] for n in 1:3)
        @test all(n in df[!, "Q_g"] for n in 10:10:30)
        @test size(df[df.num_workers.==1, :], 1) == 3
        @test size(df[df.Q_g.==20, :], 1) == 3
    end
end

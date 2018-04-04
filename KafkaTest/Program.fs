open System
open System.Text
open System.Threading
open Confluent.Kafka
open Confluent.Kafka.Serialization

[<EntryPoint>]
let main argv =
    printfn "%A" argv

    let uuid = Guid.NewGuid().ToString()
    let servers = argv |> Array.toList
    let topic = sprintf "fsharp-testing-topic-%s" uuid
    let pollTimeout = TimeSpan.FromMilliseconds 500.0

    printfn "topic=%s" topic

    let pConfig =
        Map [
            "bootstrap.servers", servers :> obj
            "compression.codec", "lz4" :> obj
        ]

    use producer = new Producer<_, _> (pConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8))
    let pTasks =
        [for i in 1 .. 100000 ->
            let s = i * i
            let vs = [for _ in 1 .. 100 -> sprintf "square=%d;" s]
            producer.ProduceAsync (topic, i.ToString(), String.Concat vs)]


    let cConfig =
        Map [
            "bootstrap.servers", servers :> obj
            "group.id", "ff-data-" + uuid :> obj
            "auto.offset.reset", "earliest" :> obj
        ]

    let loop =
        async {
            use consumer = new Consumer<_,_>(cConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8))
            consumer.Subscribe topic
            consumer.OnMessage.Add (fun msg -> printfn "key=%s" msg.Key)
            while true do
                consumer.Poll pollTimeout
        }

    Async.RunSynchronously(loop, 600000)

    0 // return an integer exit code

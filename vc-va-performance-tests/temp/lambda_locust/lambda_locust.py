import invokust

settings = invokust.create_settings(
    locustfile='locustfile.py',
    host='http://example.com',
    num_requests=10,
    num_clients=1,
    hatch_rate=1
    )

loadtest = invokust.LocustLoadTest(settings)
loadtest.run()
loadtest.stats()
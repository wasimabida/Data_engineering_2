config = {
    "openai": {
        "api_key":"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"
    },
    "kafka":{
        "sasl.username": "6K57MAYJV67MFNXM",
        "sasl.password": "6gMciUQglVobXd5wbtaIL5BkgTHdS5LivO7xycIvBcHkZIAJKSU9fmndIvC4ID4L",
        "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'session.timeout.ms':50000
    },
    "schema_registry": {
        "url": "https://psrc-r3wv7.us-central1.gcp.confluent.cloud",
        "basic.auth.user.info": "64WIRZDVUHBHHO5J:ap0ayiJOgRmBTe0Pe5YNli2VoHEbAOVbpyJYZL2qXaPn0xE14RAf2yNZ3Nxs90oY" #username:password of the schema registry 
    }
}
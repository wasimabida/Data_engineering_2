config = {
    "openai": {
        "api_key":"OpenAI_Key"
    },
    "kafka":{
        "sasl.username": "Kafka_cluster_API_key_Confluent",
        "sasl.password": "Kafka_cluster_API_secret_Confluent",
        "bootstrap.servers": "Kafka_cluster_url_bootstrap_servers:port",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'session.timeout.ms':50000
    },
    "schema_registry": {
        "url": "schema_registry_URL",
        "basic.auth.user.info": "schema_registry_api_key:schema_registry_api_secret" #username:password of the schema registry 
    }
}

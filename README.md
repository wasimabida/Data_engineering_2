# Sentiment Analysis and Real-time Streaming with OpenAI GPT-3.5 Turbo and Apache Spark

🔍This project combines the power of OpenAI GPT-3.5 Turbo for sentiment analysis with real-time streaming using Apache Spark. It classifies comments into positive, negative, or neutral categories using the GPT-3.5 Turbo model and streams the processed data to a Kafka topic.

## Architecture
![](src/assets/System_architecture.png)

Let's dive on the steps included: 
- **Setting up and Configuring TCP/IP for Data Transmission over Socket** 
- **Streaming Data with Apache Spark from Socket**
- **Real-time Sentiment Analysis with OpenAI LLM (ChatGPT):** Utilize OpenAI's Language Model (LLM), specifically ChatGPT, for real-time sentiment analysis. This involves prompt engineering to tailor the model to classify sentiment
- **Prompt Engineering:** Understand the art of crafting effective prompts to guide the language model and extract desired responses.
- **Setting up Kafka for Real-time Data Ingestion and Distribution:** Implement Kafka as a reliable and scalable message broker to facilitate real-time data ingestion and distribution.
- **Using Elasticsearch for Efficient Data Indexing and Search Capabilities:** Learn how to integrate Elasticsearch for efficient data indexing and powerful search capabilities, enhancing data exploration.

## Technologies used 
- Apache Spark
- OpenAI Language Model (ChatGPT)
- Kafka
- Elasticsearch
- Python
- TCP/IP
- Git and GitHub

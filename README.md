# <div align ="center"> Realtime Election Analysis </div>
This project focuses on building a robust system to generate real-time insights into election voting statistics and leaderboards, fostering a transparent, efficient, and effective voting process. The architecture ensures seamless data streaming, processing, and visualization to deliver real-time updates to end-users.

- **Real-Time Insights**: Continuously updated voting statistics and leaderboards for instant visibility into election progress.
- **Transparency**: Promotes fairness and openness by providing stakeholders with up-to-date and accurate data.
- **Scalability and Efficiency**: Designed to handle high volumes of concurrent data without compromising performance.

## Technologies Used

<div align="center">
  <img src="https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Spark-FF3300?style=for-the-badge&logo=apacheapache&logoColor=white" />
  <img src="https://img.shields.io/badge/Streamlit-FF4B24?style=for-the-badge&logo=streamlit&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
</div>
</div>

- **Kafka**: For real-time data streaming and message brokering.
- **PostgreSQL**: For storing and querying election data.
- **Python**: The primary programming language used for data processing and analysis.
- **Spark Streaming**: For processing data in real-time and performing analytics.
- **Streamlit**: For creating the interactive web application.
- **Docker**: Docker is used for containerization, which simplifies deployment and ensures that the application runs consistently across different environments.

## Features

- **Real-time Data Visualization**: Get live updates on voting statistics and metrics.
- **Dynamic Charts**: Visualize data using pie charts and bar charts for better insights.
- **User-friendly Interface**: Easy navigation through the dashboard for viewing election data.
- **Custom Refresh Interval**: Users can set a refresh interval for real-time data updates.

![app_demo](https://github.com/user-attachments/assets/52db3e2b-20a1-4adc-9abe-32beb073d271)

 ## System Architecture
 
<img src="https://github.com/user-attachments/assets/c3e81a12-284a-42df-84a4-d3018092f01e" alt="system architecture" width="100%" height="300" style="margin: 10px;"/>

## Data Model
<img src="https://github.com/user-attachments/assets/100bd8da-932c-4d4d-9bb3-95ce2bf5f648" alt="data_model" width="50%" height="600" style="margin: 10px;"/>

## Project Setup

### Prerequisites
- Python 3.9 or above installed on your machine
- Docker Compose installed on your machine
- Docker installed on your machine

### Steps to setup environment
1. Clone this repository.
2. Navigate to the root containing the Docker Compose file.
3. Run the following command start Zookeeper, Kafka and Postgres containers in detached mode 
```bash
docker-compose up -d
```
4. Setup a Virtual environment 
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```
5. Install the required packages
```bash
pip install -r requirements.txt
```
6. Update config.json as per your system
```bash
{
    // Database credentials
    "database": {
        "host": "localhost",
        "username": "election_user",
        "password": "election_pass",
        "db_name": "voting",
        "port": 5433
    },
    "tables": ["candidates", "voters", "votes"], # List of tables
    "randomuser_url": "https://randomuser.me/api/?nat=in", # RandomUser API Base URL
    "parties": ["BJP", "INC", "TDP", "BSP", "SP", "AAP"], # List of Political Parties
    "total_candidates": 12, # configuration for total candidates 
    "total_voters": 1000, # configuration for total voters
    "voting_interval": 0.5, # configuration for voting simulation interval
    "kafka_topics": {
        "votes_topic": "votes_topic" # List of kafka topics
    },
    "base_dir": "/Users/naman/Desktop/DataEngineering/RealtimeElectionPipeline/" # Base directory or root path
}
```

7. Run setup.py to create Postgres tables and generate data
```bash
python3 setup.py
```

### Steps to Run the App
Terminal 1 -> Consuming the voter information from Postgres, generating voting data and producing voting data to the Kafka topic:
```bash
python3 voting_app.py
```

Terminal 2 -> Spark streaming Jobs consuming the voting data from Kafka topic, enriching the data, calculate aggregates and producing data to specific topics on Kafka:
```bash
python3 spark-streaming.py
```

Terminal 3 -> Running the Streamlit app:
```bash
streamlit run streamlit_app/app.py
```

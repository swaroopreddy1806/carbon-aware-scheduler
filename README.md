# Carbon Aware Scheduler (AWS)

This project implements a carbon-aware job scheduling system using AWS services.

The system optimizes where and when jobs run based on carbon intensity data to reduce environmental impact.

## Architecture

User uploads job file → Amazon S3  
S3 triggers → AWS Lambda  
Lambda analyzes carbon intensity  
Job scheduled in region with lowest emissions

## Technologies Used

- Python
- AWS Lambda
- Amazon S3
- Amazon RDS
- Boto3

## Features

- Carbon intensity based workload optimization
- Multi-region scheduling logic
- Serverless event-driven architecture
- CSV job input processing

## Author

Swaroop Reddy








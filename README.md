# SLaMO - Seasonal Labor Market Observatory

## Description


## Creating and Running Pipeline Locally
To run the pipeline locally, use the following command:
`python -m dags`

## Creating and Running Migrations
To automatically create a new migration, use the following command: 
`piccolo migrations new db --auto`

To run the migrations, use the following command:
`piccolo migrations forwards db`


## Running the Backend API
To run the backend API, use the following command:
`uvicorn backend.app:app --reload`
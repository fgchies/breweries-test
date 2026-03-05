FROM python:3.12-slim

WORKDIR /app

#install dependencies 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#copy project
COPY pyproject.toml .
COPY README.md .

# copy source
COPY src ./src

#install
RUN pip install -e .

ENV DATA_DIR=/app/data

RUN mkdir -p /app/data/bronze /app/data/silver /app/data/gold

ENTRYPOINT ["python", "-m", "breweries_pipeline.cli"]
FROM  docker.io/bitnami/spark:3.5.0


# Set the working directory in the container
WORKDIR /app

# Install Java
#RUN apt-get update && apt-get install -y default-jdk

# Copy the requirements file into the container
COPY requirements.txt .

# Install the necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt


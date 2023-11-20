# Use an official Python runtime as a base image
FROM python:3.13-rc-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the entire "order-generator" folder into the container
COPY order-generator /app/order-generator

# Install required packages from requirements.txt
RUN pip install -r /app/order-generator/requirements.txt

# Copy the "data" folder into the container
COPY data /app/data

# Set the Python script as the entrypoint and pass CMD arguments as parameters
ENTRYPOINT ["python", "/app/order-generator/sample3.py"]

# Default arguments to be passed to the Python script
CMD ["--customers", "/app/data/Customers.csv", "--products", "/app/data/Products.csv"]

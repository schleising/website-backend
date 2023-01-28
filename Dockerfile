# Start with the Python Alpine image as the build image
FROM python:3.11-alpine as build

# Install build-base and libffi headers
RUN apk update && \
    apk add build-base libffi-dev git

# Add the required packages, disabling cache to reduce the size of the image
COPY ./requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

# Start witha fresh image after installing the necessary Python packages
FROM python:3.11-alpine

# Copy the python packages only from the build stage
COPY --from=build /root/.local /root/.local

# Make sure scripts in .local are in the PATH
ENV PATH=/root/.local/bin:$PATH

# Set the entrypoint to run uvicorn with a socket connection
CMD [ "python", "src/main.py" ]

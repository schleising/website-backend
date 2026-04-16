# Start with the Python Alpine image as the build image
FROM python:3-alpine AS build

# Install build-base and libffi headers
RUN apk update && \
    apk add build-base libffi-dev git

# Add the required packages, disabling cache to reduce the size of the image
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r /requirements.txt

# Start witha fresh image after installing the necessary Python packages
FROM python:3-alpine

# Copy the python packages only from the build stage
COPY --from=build /install /usr/local

# Create an unprivileged runtime user
RUN addgroup -S app && adduser -S -G app app

# Run as non-root
USER app

# Set the entrypoint to run uvicorn with a socket connection
CMD [ "python", "src/main.py" ]

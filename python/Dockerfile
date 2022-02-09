FROM python:3.8-slim

ARG pypi_repo
ENV PIP_EXTRA_INDEX_URL=$pypi_repo
ENV APP_HOME /app
ENV SCISPACY_CACHE=$APP_HOME/.scispacy
WORKDIR $APP_HOME

# Install production dependencies.

COPY requirements.txt ./
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy local code to container image
COPY . ./

CMD ["mlserver", "start", "model"]

FROM python:3.8

# Dependencies for building wheels during poetry install
ENV BUILD_DEPS curl

WORKDIR /app

# Install Python dependencies using poetry
COPY pyproject.toml poetry.lock ./
ENV PATH="/root/.poetry/bin:$PATH"
RUN apt-get update
RUN apt-get install -y --no-install-recommends ${BUILD_DEPS}
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
RUN poetry config virtualenvs.create false
RUN poetry install
RUN apt-get remove --purge -y --autoremove ${BUILD_DEPS}
RUN rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["python", "get_daily_message.py"]

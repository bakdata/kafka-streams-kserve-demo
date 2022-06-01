FROM python:3.8-slim

COPY requirements.txt ./
RUN pip install --no-cache-dir -r ./requirements.txt

COPY . ./

CMD ["mlserver", "start", "argos-translator-en-es"]

FROM python:3.8-slim
WORKDIR /usr/src/app
COPY req.txt ./
RUN pip install --no-cache-dir -r req.txt
EXPOSE 8084
COPY . .
CMD [ "python", "app.py" ]
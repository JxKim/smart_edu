FROM python:3.10-slim
WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=appuser:appuser . .
EXPOSE 9005
CMD ["python", "src/web/app.py"]
USER appuser
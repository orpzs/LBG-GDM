include env.sh
export

install:
	@poetry config virtualenvs.in-project true; poetry install

pre-commit:
	@pre-commit run --all-files


run:
	poetry run uvicorn inference_workflow.api.app:app --reload

build:
	@eval $$(python config/load_env.py) && \
	docker build \
	-t $$TEMPLATES_IMAGE_URI . && \
	docker push $$TEMPLATES_IMAGE_URI

build-load-test:
	docker build \
	-t ${LOAD_TEST_IMAGE_URI} \
	--build-arg BASE_IMAGE_URI=${LOAD_TEST_BASE_IMAGE_URI} \
	--build-arg PYTHON_INDEX_URL=${PYTHON_INDEX_URL} \
	testing/load_test/
	docker push ${LOAD_TEST_IMAGE_URI}

run-load-test:
	gcloud beta run jobs update load-test \
	--vpc-egress=all-traffic \
	--parallelism=1 \
	--max-retries=1 \
	--task-timeout=1800 \
	--cpu=1000m \
	--memory=512Mi \
	--image=${LOAD_TEST_IMAGE_URI} \
	--region=${REGION} \
	--service-account=${LOAD_TEST_SERVICE_ACCOUNT} \
	--network=${LOAD_TEST_NETWORK} \
	--subnet=${LOAD_TEST_SUBNET} \
	--set-env-vars=LOCUST_LOCUSTFILE=${LOCUST_LOCUSTFILE},LOCUST_HOST=${LOCUST_HOST},LOCUST_CSV=${LOCUST_CSV},LOCUST_HTML=${LOCUST_HTML},LOCUST_HEADLESS=${LOCUST_HEADLESS},LOCUST_USERS=${LOCUST_USERS},LOCUST_SPAWN_RATE=${LOCUST_SPAWN_RATE},LOCUST_EXPECT_WORKERS=${LOCUST_EXPECT_WORKERS},LOCUST_RUN_TIME=${LOCUST_RUN_TIME},LOAD_TEST_BUCKET=${LOAD_TEST_BUCKET},LOAD_TEST_INPUT=${LOAD_TEST_INPUT} \
	&& gcloud run jobs execute load-test


pytest:
	pytest


dev-backend:
	poetry run uvicorn agents.main:app --reload --port=8000

dev-frontend:
	poetry run streamlit run frontend/streamlit_app.py


build-backend:
	@eval $$(python config/load_env.py) && \
	gcloud builds submit --tag "${REGION}-docker.pkg.dev/${PROJECT_ID}/usecases/gdm-backend:latest" .

deploy-backend:
	@eval $$(python config/load_env.py) && \
	gcloud run deploy "gdm-backend" \
  --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/usecases/gdm-backend:latest" \
  --platform managed \
  --region "${REGION}" \
  --port 8080 \
  --allow-unauthenticated \
  --set-env-vars=CONFIG_PATH=config.yaml \
  --timeout=2000 \
  --memory=2Gi


build-frontend:
	@eval $$(python config/load_env.py) && \
	gcloud builds submit --config frontend.yaml . \
	  --substitutions=_REGION="${REGION}",_PROJECT_ID="${PROJECT_ID}"

deploy-frontend:
	@eval $$(python config/load_env.py) && \
	export BACKEND_URL=$$(gcloud run services describe gdm-backend --platform managed --region ${REGION} --format 'value(status.url)') && \
	gcloud run deploy "gdm-frontend" \
	  --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/usecases/gdm-frontend:latest" \
	  --platform managed \
	  --region "${REGION}" \
	  --port 8000 \
	  --allow-unauthenticated \
	  --set-env-vars=API_BASE_URL=$$BACKEND_URL \
	  --timeout=2000 \
	  --memory=2Gi

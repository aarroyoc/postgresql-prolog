test: psql-up && psql-down
	logtalk_tester -p scryer

psql-up:
	docker-compose up -d postgres

psql-down:
	docker-compose down

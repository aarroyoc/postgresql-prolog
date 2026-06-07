test: psql-up && psql-down
	logtalk_tester -p scryer -o verbose

test-scram: psql-up
	DATABASE_HOST=127.0.0.1 \
	DATABASE_PORT=5433 \
	DATABASE_USERNAME=postgres \
	DATABASE_PASSWORD=postgres \
	DATABASE_DB_NAME=postgres \
	scryer-prolog ./scram_test.pl -g 'run_test'

test-scram: psql-up
	scryer-prolog ./scram_test.pl -g 'run_test'

psql-up:
	docker-compose up -d postgres postgres-scram

psql-down:
	docker-compose down

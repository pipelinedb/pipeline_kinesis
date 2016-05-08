MODULE_big = pipeline_kinesis
OBJS = pipeline_kinesis.o kinesis_consumer.o

EXTENSION = pipeline_kinesis
DATA = pipeline_kinesis--0.9.0.sql

SHLIB_LINK = -laws-cpp-sdk-core \
	-laws-cpp-sdk-kinesis \
	-lpthread

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

kinesis_consumer.o: kinesis_consumer.cpp kinesis_consumer.h conc_queue.hpp
	g++ \
	-g \
	-DAWS_CUSTOM_MEMORY_MANAGEMENT \
	-DAWS_SDK_PLATFORM_LINUX \
	-DENABLE_CURL_CLIENT \
	-DENABLE_OPENSSL_ENCRYPTION \
	-DJSON_USE_EXCEPTION=0 \
	-Wall \
	-c \
	-fPIC \
	kinesis_consumer.cpp \
	-std=c++11 \
	-Wextra \
	-Wno-comment \
	-o kinesis_consumer.o

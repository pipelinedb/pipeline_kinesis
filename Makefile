MODE := Debug

all: pipeline_kinesis

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
	kinesis_consumer.cpp \
	-std=c++11 \
	-Wextra \
	-Wno-comment \
	-o kinesis_consumer.o

main.o: main.c kinesis_consumer.h
	gcc -g -c main.c -o main.o

pipeline_kinesis: main.o kinesis_consumer.o conc_queue.hpp
	g++ -g main.o kinesis_consumer.o -o pipeline_kinesis \
		-L/usr/local/lib/linux/intel64/$(MODE) \
	-laws-cpp-sdk-core \
	-laws-cpp-sdk-kinesis \
	-lpthread

clean:
	rm -f *.o pipeline_kinesis

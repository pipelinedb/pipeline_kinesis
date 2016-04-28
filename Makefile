all: pipeline_kinesis

reader.o: reader.cpp conc_queue.h
	g++ \
		-g \
	-DAWS_CUSTOM_MEMORY_MANAGEMENT \
	-DAWS_SDK_PLATFORM_LINUX \
	-DENABLE_CURL_CLIENT \
	-DENABLE_OPENSSL_ENCRYPTION \
	-DJSON_USE_EXCEPTION=0 \
	-Wall \
	-O0 \
	-c \
	reader.cpp \
	-std=c++11 \
	-Wextra \
	-Wno-comment \
	-I/home/jason/usr/local/include \
	-o reader.o

main.o: main.c
	gcc -g -c main.c -o main.o

pipeline_kinesis: main.o reader.o
	g++ -g main.o reader.o -o pipeline_kinesis \
	-L/home/jason/usr/local/lib/linux/intel64/ \
	-laws-cpp-sdk-core \
	-laws-cpp-sdk-kinesis \
	-lpthread

clean:
	rm *.o pipeline_kinesis

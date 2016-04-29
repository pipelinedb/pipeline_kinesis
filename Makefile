#AWS_REPO := https://github.com/aws/aws-sdk-cpp
#
#aws_sdk:
#	git clone $(AWS_REPO)

all: pipeline_kinesis

reader.o: reader.cpp conc_queue.h
	g++ \
	-DAWS_CUSTOM_MEMORY_MANAGEMENT \
	-DAWS_SDK_PLATFORM_LINUX \
	-DENABLE_CURL_CLIENT \
	-DENABLE_OPENSSL_ENCRYPTION \
	-DJSON_USE_EXCEPTION=0 \
	-Wall \
	-c \
	reader.cpp \
	-std=c++11 \
	-Wextra \
	-Wno-comment \
	-o reader.o

main.o: main.c
	gcc -g -c main.c -o main.o

pipeline_kinesis: main.o reader.o conc_queue.h
	g++ -g main.o reader.o -o pipeline_kinesis \
		-L/usr/local/lib/linux/intel64/Release \
	-laws-cpp-sdk-core \
	-laws-cpp-sdk-kinesis \
	-lpthread

clean:
	rm -f *.o pipeline_kinesis

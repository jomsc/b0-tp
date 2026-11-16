#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include "messageAdder.h"
#include "msg.h"
#include "iMessageAdder.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"

//consumer thread
pthread_t consumer;
//Message computed
volatile MSG_BLOCK out;
//Consumer count storage
volatile unsigned int consumeCount = 0;
//Protects out and consumeCount together
static pthread_mutex_t sumMutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Increments the consume count.
 */
static void incrementConsumeCount(void);

/**
 * Consumer entry point.
 */
static void *sum( void *parameters );


static void incrementConsumeCount(void){
	//Called with sumMutex held
	consumeCount++;
}

MSG_BLOCK getCurrentSum(){
	MSG_BLOCK s;
	pthread_mutex_lock(&sumMutex);
	s = out;
	pthread_mutex_unlock(&sumMutex);
	return s;
}

unsigned int getConsumedCount(){
	unsigned int c;
	pthread_mutex_lock(&sumMutex);
	c = consumeCount;
	pthread_mutex_unlock(&sumMutex);
	return c;
}


void messageAdderInit(void){
	out.checksum = 0;
	for (size_t i = 0; i < DATA_SIZE; i++)
	{
		out.mData[i] = 0;
	}
	pthread_create(&consumer, NULL, sum, NULL);
}

void messageAdderJoin(void){
	pthread_join(consumer, NULL);
	pthread_mutex_destroy(&sumMutex);
}

static void *sum( void *parameters )
{
	(void)parameters;
	MSG_BLOCK msg;
	D(printf("[messageAdder]Thread created for sum\n"));
	unsigned int i = 0;
	while(i<ADDER_LOOP_LIMIT){
		i++;
		sleep(ADDER_SLEEP_TIME);
		//Blocking until a message is available
		msg = getMessage();
		//Sum and count updated in the same critical section (coherence)
		pthread_mutex_lock(&sumMutex);
		messageAdd(&out, &msg);
		incrementConsumeCount();
		pthread_mutex_unlock(&sumMutex);
	}
	printf("[messageAdder] termination\n");
	pthread_exit(NULL);
}

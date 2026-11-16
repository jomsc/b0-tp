#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include "acquisitionManager.h"
#include "msg.h"
#include "iSensor.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"

//Size of the shared message buffer
#define BUFFER_SIZE 8

//producer count storage
volatile unsigned int produceCount = 0;

pthread_t producers[PRODUCER_COUNT];
//Producer ids passed to the threads
static unsigned int producerIds[PRODUCER_COUNT];

//Shared message buffer (circular)
static volatile MSG_BLOCK buffer[BUFFER_SIZE];
//Next slot to write (shared by the producers, protected by writeIdxMutex)
static unsigned int writeIdx = 0;
//Next slot to read (used by the single consumer only)
static unsigned int readIdx = 0;

static void *produce(void *params);

/**
* Semaphores and Mutex
*/
//Counts the free slots of the buffer (init BUFFER_SIZE)
static sem_t *semFree;
//One event per slot: the slot is filled (init 0)
static sem_t *slotFull[BUFFER_SIZE];
//Protects the write index reservation
static pthread_mutex_t writeIdxMutex = PTHREAD_MUTEX_INITIALIZER;
//Protects produceCount
static pthread_mutex_t countMutex = PTHREAD_MUTEX_INITIALIZER;

#define SEM_FREE_NAME "/acq_semFree"
#define SEM_FULL_NAME_FORMAT "/acq_slotFull_%u"

/*
* Creates the synchronization elements.
* @return ERROR_SUCCESS if the init is ok, ERROR_INIT otherwise
*/
static unsigned int createSynchronizationObjects(void);

/*
* Increments the produce count.
*/
static void incrementProducedCount(void);

static unsigned int createSynchronizationObjects(void)
{
	unsigned int i;
	char name[32];

	sem_unlink(SEM_FREE_NAME);
	semFree = sem_open(SEM_FREE_NAME, O_CREAT, 0644, BUFFER_SIZE);
	if (semFree == SEM_FAILED)
	{
		perror("[acquisitionManager]sem_open semFree");
		return ERROR_INIT;
	}

	for (i = 0; i < BUFFER_SIZE; i++)
	{
		snprintf(name, sizeof(name), SEM_FULL_NAME_FORMAT, i);
		sem_unlink(name);
		slotFull[i] = sem_open(name, O_CREAT, 0644, 0);
		if (slotFull[i] == SEM_FAILED)
		{
			perror("[acquisitionManager]sem_open slotFull");
			return ERROR_INIT;
		}
	}
	printf("[acquisitionManager]Semaphore created\n");
	return ERROR_SUCCESS;
}

static void incrementProducedCount(void)
{
	pthread_mutex_lock(&countMutex);
	produceCount++;
	pthread_mutex_unlock(&countMutex);
}

unsigned int getProducedCount(void)
{
	unsigned int p = 0;
	pthread_mutex_lock(&countMutex);
	p = produceCount;
	pthread_mutex_unlock(&countMutex);
	return p;
}

MSG_BLOCK getMessage(void){
	MSG_BLOCK msg;
	//Wait until the next slot is filled
	sem_wait(slotFull[readIdx]);
	msg = buffer[readIdx];
	readIdx = (readIdx + 1) % BUFFER_SIZE;
	//The slot is free again
	sem_post(semFree);
	return msg;
}

unsigned int acquisitionManagerInit(void)
{
	unsigned int i;
	printf("[acquisitionManager]Synchronization initialization in progress...\n");
	fflush( stdout );
	if (createSynchronizationObjects() == ERROR_INIT)
		return ERROR_INIT;

	printf("[acquisitionManager]Synchronization initialization done.\n");

	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		producerIds[i] = i;
		if (pthread_create(&producers[i], NULL, produce, &producerIds[i]) != 0)
			return ERROR_INIT;
	}

	return ERROR_SUCCESS;
}

void acquisitionManagerJoin(void)
{
	unsigned int i;
	char name[32];
	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		pthread_join(producers[i], NULL);
	}

	sem_close(semFree);
	sem_unlink(SEM_FREE_NAME);
	for (i = 0; i < BUFFER_SIZE; i++)
	{
		snprintf(name, sizeof(name), SEM_FULL_NAME_FORMAT, i);
		sem_close(slotFull[i]);
		sem_unlink(name);
	}
	pthread_mutex_destroy(&writeIdxMutex);
	pthread_mutex_destroy(&countMutex);
	printf("[acquisitionManager]Semaphore cleaned\n");
}

void *produce(void* params)
{
	unsigned int id = *(unsigned int *)params;
	unsigned int idx;
	MSG_BLOCK local;
	D(printf("[acquisitionManager]Producer created with id %u\n", id));
	unsigned int i = 0;
	while (i < PRODUCER_LOOP_LIMIT)
	{
		i++;
		sleep(PRODUCER_SLEEP_TIME+(rand() % 5));

		//Acquisition and check (Exigence 3)
		getInput(id, &local);
		if (!messageCheck(&local))
		{
			printf("[acquisitionManager]ERROR: corrupted message on input %u, message dropped\n", id);
			continue;
		}

		//Wait for a free slot
		sem_wait(semFree);

		//Minimal critical section: reserve a slot
		pthread_mutex_lock(&writeIdxMutex);
		idx = writeIdx;
		writeIdx = (writeIdx + 1) % BUFFER_SIZE;
		pthread_mutex_unlock(&writeIdxMutex);

		//Write outside of the critical section (parallel writers)
		buffer[idx] = local;

		//Count before signaling: produced >= consumed at any time
		incrementProducedCount();
		sem_post(slotFull[idx]);
	}
	printf("[acquisitionManager] producer %u termination\n", id);
	pthread_exit(NULL);
}

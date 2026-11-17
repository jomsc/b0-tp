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
//Counts the filled slots of the buffer (init 0)
static sem_t *semFull;
//Protects the slot reservation and the slot write: slots are filled in index order
static pthread_mutex_t writeIdxMutex = PTHREAD_MUTEX_INITIALIZER;
//Protects produceCount
static pthread_mutex_t countMutex = PTHREAD_MUTEX_INITIALIZER;

#define SEM_FREE_NAME "/acq_semFree"
#define SEM_FULL_NAME "/acq_semFull"

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
	sem_unlink(SEM_FREE_NAME);
	semFree = sem_open(SEM_FREE_NAME, O_CREAT, 0644, BUFFER_SIZE);
	if (semFree == SEM_FAILED)
	{
		perror("[acquisitionManager]sem_open semFree");
		return ERROR_INIT;
	}

	sem_unlink(SEM_FULL_NAME);
	semFull = sem_open(SEM_FULL_NAME, O_CREAT, 0644, 0);
	if (semFull == SEM_FAILED)
	{
		perror("[acquisitionManager]sem_open semFull");
		return ERROR_INIT;
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
	//Wait until at least one slot is filled. Slots are filled in index
	//order (write under writeIdxMutex), so the slot at readIdx is complete.
	sem_wait(semFull);
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
	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		pthread_join(producers[i], NULL);
	}

	sem_close(semFree);
	sem_unlink(SEM_FREE_NAME);
	sem_close(semFull);
	sem_unlink(SEM_FULL_NAME);
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

		//Critical section: reserve a slot and write it, so that slots
		//are always completed in index order (required by semFull)
		pthread_mutex_lock(&writeIdxMutex);
		idx = writeIdx;
		writeIdx = (writeIdx + 1) % BUFFER_SIZE;
		buffer[idx] = local;
		pthread_mutex_unlock(&writeIdxMutex);

		//Count before signaling: produced >= consumed at any time
		incrementProducedCount();
		sem_post(semFull);
	}
	printf("[acquisitionManager] producer %u termination\n", id);
	pthread_exit(NULL);
}

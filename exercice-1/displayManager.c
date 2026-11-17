#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include "displayManager.h"
#include "iDisplay.h"
#include "iAcquisitionManager.h"
#include "iMessageAdder.h"
#include "msg.h"
#include "multitaskingAccumulator.h"
#include "debug.h"

//Total number of messages expected
#define TOTAL_MESSAGES (PRODUCER_LOOP_LIMIT * PRODUCER_COUNT)

// DisplayManager thread.
pthread_t displayThread;

/**
 * Display manager entry point.
 * */
static void *display( void *parameters );


void displayManagerInit(void){
	pthread_create(&displayThread, NULL, display, NULL);
}

void displayManagerJoin(void){
	pthread_join(displayThread, NULL);
}

static void *display( void *parameters )
{
	(void)parameters;
	MSG_BLOCK s;
	unsigned int c1, c2, p;
	D(printf("[displayManager]Thread created for display\n"));
	unsigned int consumed = 0;
	while(consumed < TOTAL_MESSAGES){
		sleep(DISPLAY_SLEEP_TIME);

		do {
			c1 = getConsumedCount();
			s = getCurrentSum();
			c2 = getConsumedCount();
		} while (c1 != c2);
		//Read last: produced >= consumed
		p = getProducedCount();

		messageDisplay(&s);
		print(p, c1);
		consumed = c1;
	}
	printf("[displayManager] termination\n");
	pthread_exit(NULL);
}

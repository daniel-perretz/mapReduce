//
// Created by dp_16cse on 5/17/23.
//


#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include "Barrier.h"
#include <algorithm>
#include <iostream>


struct JobContext;

struct ThreadContext {
    int threadId;
    JobContext * mainContext;
    IntermediateVec intermediateVec;
};


struct JobContext {
    std::atomic<uint64_t> *processedMap;
    std::atomic<uint64_t> *processedShuffle;
    std::atomic<uint64_t> *processedReduce;
    std::atomic<uint64_t> * mapIndexValue;
    int threadsNum;
    pthread_t *threads;
    const InputVec inputVec;
    OutputVec *outputVec;
    stage_t stage;
    uint64_t totalCount;
    ThreadContext *contexts;
    const MapReduceClient &client;
    pthread_mutex_t waitMutex;
    pthread_mutex_t reduceMutex;
    pthread_mutex_t emit3Mutex;
    pthread_mutex_t getJobMutex;
    pthread_mutex_t mapMutex;
    bool isDoneJoining;
    Barrier *barrier;
    std::vector<IntermediateVec> shuffledVec;
};


void validateAlloc(void *object) {
    if (object == nullptr) {
        std::cout << "system error: failed to allocate object" << std::endl;
        exit(EXIT_FAILURE);
    }
}


void initMutex(pthread_mutex_t *mutex) {
    if (pthread_mutex_init(mutex, nullptr) != 0) {
        std::cout << "system error: failed to init mutex" << std::endl;
        exit(EXIT_FAILURE);
    }
}


void destroyMutex(pthread_mutex_t *mutex) {
    if (pthread_mutex_destroy(mutex) != 0) {
        std::cout << "system error: failed to destroy mutex" << std::endl;
        exit(EXIT_FAILURE);
    }
}


void lockMutex(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex) != 0) {
        std::cout << "system error: failed to lock mutex" << std::endl;
        exit(EXIT_FAILURE);
    }
}


void unlockMutex(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex) != 0) {
        std::cout << "system error: failed to unlock mutex" << std::endl;
        exit(EXIT_FAILURE);
    }
}


bool equalKeys (const std::pair<K2*, V2*> &p1, const std::pair<K2*, V2*> &p2) {
    return !((*p1.first) < (*p2.first)) && ! ((*p2.first) < (*p1.first));
}


bool compareKeys (const std::pair<K2*, V2*> &p1, const std::pair<K2*, V2*> &p2) {
    return ((*p1.first) < (*p2.first));
}


void* entry_point(void* arg) {
    auto* tc = (ThreadContext*) arg;
    uint64_t old_value;
    tc->mainContext->stage = MAP_STAGE;

    while((*tc->mainContext->mapIndexValue) < tc->mainContext->inputVec.size()) {
        lockMutex(&tc->mainContext->mapMutex);
        old_value = (*(tc->mainContext->mapIndexValue))++;

        if(old_value >= tc->mainContext->inputVec.size()) {
            unlockMutex(&tc->mainContext->mapMutex);
            break;
        }

        auto p = tc->mainContext->inputVec[old_value];
        (*tc->mainContext->processedMap)++;
        unlockMutex(&tc->mainContext->mapMutex);

        tc->mainContext->client.map(p.first, p.second, &tc->intermediateVec);
    }

    //sort phase
    std::sort(tc->intermediateVec.begin(),tc->intermediateVec.end(),compareKeys);

    //shuffle phase
    tc->mainContext->barrier->barrier();

    if(tc->threadId == 0) {
        for (int i = 0; i < tc->mainContext->threadsNum; ++i) {
            tc->mainContext->totalCount += tc->mainContext->contexts[i].intermediateVec.size();
        }

        tc->mainContext->stage = SHUFFLE_STAGE;

        while(((*tc->mainContext->processedShuffle)) < tc->mainContext->totalCount) {
            IntermediatePair maxPair;
            IntermediateVec tmpVec;
            for (int i = 0; i < tc->mainContext->threadsNum; ++i) {
                if(!tc->mainContext->contexts[i].intermediateVec.empty()) {
                    maxPair = tc->mainContext->contexts[i].intermediateVec.back();
                    break;
                }
            }

            // finding the maximum letter
            for (int i = 0; i < tc->mainContext->threadsNum; ++i) {
                if(!tc->mainContext->contexts[i].intermediateVec.empty()) {
                    if(compareKeys(maxPair,tc->mainContext->contexts[i].intermediateVec.back()))
                        maxPair = tc->mainContext->contexts[i].intermediateVec.back();
                }
            }

            for (int i = 0; i < tc->mainContext->threadsNum; ++i) {
                while(!tc->mainContext->contexts[i].intermediateVec.empty() &&
                      equalKeys(maxPair,tc->mainContext->contexts[i].intermediateVec.back())){
                    tmpVec.push_back(tc->mainContext->contexts[i].intermediateVec.back());
                    tc->mainContext->contexts[i].intermediateVec.pop_back();
                    (*tc->mainContext->processedShuffle)++;
                }
            }

            tc->mainContext->shuffledVec.push_back(tmpVec);
        }

        tc->mainContext->stage = REDUCE_STAGE;
        tc->mainContext->totalCount = tc->mainContext->shuffledVec.size();

    }

    tc->mainContext->barrier->barrier(); // wait to thread 0

    // reduce phase
    while(!(tc->mainContext->shuffledVec.empty())) {
        lockMutex(&tc->mainContext->reduceMutex);

        if(tc->mainContext->shuffledVec.empty()) {
            unlockMutex(&tc->mainContext->reduceMutex);
            break;
        }

        auto cur = tc->mainContext->shuffledVec.back();
        tc->mainContext->shuffledVec.pop_back();
        unlockMutex(&tc->mainContext->reduceMutex);
        tc->mainContext->client.reduce(&cur,tc);
        (*tc->mainContext->processedReduce)++;
    }

    return tc;
}


void emit2 (K2* key, V2* value, void* context) {
    auto *interVec = (IntermediateVec *) context;
    IntermediatePair pair;
    pair.first = key;
    pair.second = value;
    interVec->push_back(pair);
}


void emit3 (K3* key, V3* value, void* context) {
    auto *threadContext = (ThreadContext*) context;
    lockMutex(&threadContext->mainContext->emit3Mutex);
    OutputPair pair;
    pair.first = key;
    pair.second = value;
    threadContext->mainContext->outputVec->push_back(pair);
    unlockMutex(&threadContext->mainContext->emit3Mutex);
}


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec,
                            OutputVec& outputVec, int multiThreadLevel) {
    pthread_mutex_t waitMutex,reduceMutex,emit3Mutex,getJobMutex, mapMutex;
    initMutex(&waitMutex);
    initMutex(&reduceMutex);
    initMutex(&emit3Mutex);
    initMutex(&getJobMutex);
    initMutex(&mapMutex);

    auto *barrier = new(std::nothrow) Barrier(multiThreadLevel);
    validateAlloc(&barrier);

    auto *threads = new(std::nothrow) pthread_t[multiThreadLevel];
    validateAlloc(&threads);

    auto shuffledVec = std::vector<IntermediateVec>();
    auto *processedMap = new(std::nothrow)std::atomic<uint64_t>( 0);
    validateAlloc(&processedMap);

    auto *processedShuffle = new(std::nothrow)std::atomic<uint64_t>( 0);
    validateAlloc(&processedShuffle);

    auto *processedReduce = new(std::nothrow)std::atomic<uint64_t>( 0);
    validateAlloc(&processedReduce);

    auto *mapIndexValue = new(std::nothrow)std::atomic<uint64_t>( 0);
    validateAlloc(&mapIndexValue);

    auto *contexts = new(std::nothrow)ThreadContext[multiThreadLevel];
    validateAlloc(&contexts);

    auto *jobContext = new(std::nothrow) JobContext{processedMap, processedShuffle, processedReduce,
                                                    mapIndexValue, multiThreadLevel, threads,
                                                    inputVec, &outputVec, UNDEFINED_STAGE, 0,
                                                    contexts, client, waitMutex, reduceMutex,
                                                    emit3Mutex, getJobMutex, mapMutex, false,
                                                    barrier, shuffledVec};
    validateAlloc(&jobContext);

    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i].threadId = i;
        contexts[i].mainContext = jobContext;
        if(pthread_create(threads + i, nullptr, entry_point, (void*)(contexts + i)) != 0) {
            std::cout << "system error: failed to create pthread " << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    return (JobHandle) jobContext;
}


void waitForJob(JobHandle job){
    auto *jobContext = (JobContext *) job;
    lockMutex(&jobContext->waitMutex);

    if (!jobContext->isDoneJoining) {
        for (int i = 0; i < jobContext->threadsNum; ++i) {
            if(pthread_join(jobContext->threads[i], nullptr) != 0) {
                std::cout << "system error: failed to join pthread" << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        jobContext->isDoneJoining = true;
    }

    unlockMutex(&jobContext->waitMutex);
}


void getJobState(JobHandle job, JobState* state) {
    auto *jobContext = (JobContext *) job;
    lockMutex(&jobContext->getJobMutex);
    state->stage = jobContext ->stage;

    if(jobContext->stage == UNDEFINED_STAGE) {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0;
    }

    if(jobContext->stage == MAP_STAGE) {
        state->stage = MAP_STAGE;
        state->percentage = std::min((100 * (float) (*(jobContext->processedMap)) /
                (float) jobContext->inputVec.size()), 100.f);
    }

    else if (jobContext->stage == SHUFFLE_STAGE) {
        state->stage = SHUFFLE_STAGE;
        state->percentage = std::min(((100 * (float) (*(jobContext->processedShuffle)) /
                (float) jobContext->totalCount)), 100.f);
    }

    else if (jobContext->stage == REDUCE_STAGE) {
        state->stage = REDUCE_STAGE;
        state->percentage = std::min((100 * (float) (*(jobContext->processedReduce)) /
                (float) jobContext->totalCount), 100.f);
    }

    unlockMutex(&jobContext->getJobMutex);
}


void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto *jobContext = (JobContext *) job;

    delete jobContext->barrier;
    delete[] jobContext->threads;
    delete[] jobContext->contexts;
    delete jobContext->processedMap;
    delete jobContext->processedShuffle;
    delete jobContext->processedReduce;
    delete jobContext->mapIndexValue;
    jobContext->totalCount = 0;

    destroyMutex(&jobContext->waitMutex);
    destroyMutex(&jobContext->reduceMutex);
    destroyMutex(&jobContext->emit3Mutex);
    destroyMutex(&jobContext->getJobMutex);
    destroyMutex(&jobContext->mapMutex);

    delete jobContext;
}

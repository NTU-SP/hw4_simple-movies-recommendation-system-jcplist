#include "header.h"

extern int num_of_movies;

static void validate(char** movies, double* pts, int size); //validate passing arrays
static int argmax(double* src, char** movie, int start, int end); //return argmax of source
static void swap(char** movies1,char** movies2, double* pts1, double* pts2);
static char* add_secret(char* s, int length);

void sort(char** movies, double* pts, int size){
	 
	validate(movies,pts,size);

	for( int i = 0; i < size-1; i++ ){
		
		int idx = argmax(pts,movies,i,size);

		assert(idx >= 0 && idx < size);

		swap(&movies[i],&movies[idx],&pts[i],&pts[idx]);
		
		movies[i] = add_secret(movies[i],strlen(movies[i]));

	}
	
	movies[size-1] = add_secret(movies[size-1],strlen(movies[size-1]));
	
	// Network delay
	usleep(100000);
}

/* HOW TO ADD SECRET: https://www.csie.ntu.edu.tw/~b08902114/ */
/* Note that it is just a example code*/
static char* add_secret(char* s, int length){

	assert(length < MAX_LEN && length >= 0);
	char key[MAX_LEN];
   	sprintf(key,"\t[It's top secret.]");
	int l = length + strlen(key)+1;

	/*It is guranteed that the strlen of secret string will not exceed MAX_LEN*/
	char* secret = malloc(sizeof(char)*l);
	if(secret == NULL){
		ERR_EXIT("malloc");
	}	
	memcpy(secret,s,length);
	memcpy(secret+length,key,strlen(key));
	secret[l-1] = '\0';

	return secret;
}

static void validate(char** movies, double* pts, int size){
	assert(size <= num_of_movies);
	for(int i = 0;i < size; i++){
		if(movies[i] == NULL) {
			fprintf(stderr, "invalid memory\n");
			exit(-1);
		}
	}

	if( pts == NULL ){
		fprintf(stderr, "invalid memory\n");
		exit(-1);	
	}
}

static int argmax(double* src,char** movie, int start, int end){
	
	int ret = start;
	double max = -1.0;

	for( int i = start; i < end; i++ ){
		if(src[i] > max){
			max = src[i];
			ret = i;
		} else if(src[i] == max){
			if(strcmp(movie[ret],movie[i]) > 0)
				ret = i;
		}
	}
	assert(max >= 0.0);
	return ret;
}

static void swap(char** movies1,char** movies2, double* pts1, double* pts2){
	char* m = *movies1;
	*movies1 = *movies2;
	*movies2 = m;

	double p = *pts1;
	*pts1 = *pts2;
	*pts2 = p;
}

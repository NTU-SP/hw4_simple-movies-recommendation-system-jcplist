#include "header.h"

extern int num_of_movies;

static void validate(char** movies, double* pts, int size); //validate passing arrays
static int argmax(double* src, char** movie, int start, int end); //return argmax of source
static void swap(char** movies1,char** movies2, double* pts1, double* pts2);

void sort(char** movies, double* pts, int size){
	 
	validate(movies,pts,size);

	for( int i = 0; i < size-1; i++ ){
		
		int idx = argmax(pts,movies,i,size);

		assert(idx >= 0 && idx < size);

		swap(&movies[i],&movies[idx],&pts[i],&pts[idx]);

	  	/* add_secret(movies[i]) 
		 REF: https://www.csie.ntu.edu.tw/~b08902114/ */

	}
	
	// Network delay
	usleep(100000);
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


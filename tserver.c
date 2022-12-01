#include "header.h"

movie_profile* movies[MAX_MOVIES];
unsigned int num_of_movies = 0;
unsigned int num_of_reqs = 0;

// Global request queue and pointer to front of queue
// TODO: critical section to protect the global resources
request* reqs[MAX_REQ];
int front = -1;

/* Note that the maximum number of processes per workstation user is 512. * 
 * We recommend that using about <256 threads is enough in this homework. */
pthread_t tid[MAX_CPU][MAX_THREAD]; //tids for multithread

//mutex
pthread_mutex_t lock; 

void initialize(FILE* fp);
request* read_request();
int pop();

int pop(){
	front+=1;
	return front;
}

typedef struct thread_arg
{
	char **mov;
	double *pts;
	char **a;
	double *b;
	int size;
	int depth;
} thread_arg;

void *merge_sort (void *qwq)
{
	thread_arg *arg = qwq;

	if (arg->size <= 1024 || arg->depth >= 4)
	{
		sort(arg->mov, arg->pts, arg->size);
		return 0;
	}

	thread_arg left, right;
	
	left.mov = arg->mov;
	left.pts = arg->pts;
	left.a = arg->a;
	left.b = arg->b;
	left.size = arg->size / 2;
	left.depth = arg->depth + 1;

	right.mov = arg->mov + left.size;
	right.pts = arg->pts + left.size;
	right.a = arg->a + left.size;
	right.b = arg->b + left.size;
	right.size = arg->size - left.size;
	right.depth = arg->depth + 1;

	pthread_t ltid, rtid;
	if (pthread_create(&ltid, NULL, merge_sort, &left))
	{
		ERR_EXIT("can't create thread");
	}
	if (pthread_create(&rtid, NULL, merge_sort, &right))
	{
		ERR_EXIT("can't create thread");
	}

	pthread_join(ltid, NULL), pthread_join(rtid, NULL);

	int l = 0, r = 0, m = 0;
	for (; l < left.size && r < right.size;)
	{
		if (left.pts[l] > right.pts[r])
		{
			arg->a[m] = left.mov[l];
			arg->b[m] = left.pts[l];
			m++, l++;
		}
		else if (left.pts[l] < right.pts[r])
		{
			arg->a[m] = right.mov[r];
			arg->b[m] = right.pts[r];
			m++, r++;
		}
		else if (strcmp(left.mov[l], right.mov[r]) < 0)
		{
			arg->a[m] = left.mov[l];
			arg->b[m] = left.pts[l];
			m++, l++;
		}
		else
		{
			arg->a[m] = right.mov[r];
			arg->b[m] = right.pts[r];
			m++, r++;
		}
	}

	while (l < left.size)
	{
		arg->a[m] = left.mov[l];
		arg->b[m] = left.pts[l];
		m++, l++;
	}

	while (r < right.size)
	{
		arg->a[m] = right.mov[r];
		arg->b[m] = right.pts[r];
		m++, r++;
	}

	memcpy(arg->mov, arg->a, sizeof(char *) * arg->size);
	memcpy(arg->pts, arg->b, sizeof(double) * arg->size);

	return 0;
}

static inline double score (const double *a, const double *b)
{
	double r = 0;
	for (int i = 0; i < NUM_OF_GENRE; i++)
	{
		r += a[i] * b[i];
	}
	return r;
}

int filter (char ***mov, double **pts, const char *key, const double *profile)
{
	int sz = 0;
	for (int i = 0; i < num_of_movies; i++)
	{
		if (strstr(movies[i]->title, key) != NULL) sz++;
	}

	char **res = malloc(sizeof(char *) * sz);
	double *resb = malloc(sizeof(double) * sz);

	for (int i = 0, j = 0; j < sz; i++)
	{
		if (strstr(movies[i]->title, key) != NULL)
		{
			res[j] = malloc(MAX_LEN);
			strcpy(res[j], movies[i]->title);
			resb[j] = score(movies[i]->profile, profile);
			j++;
		}
	}
	*mov = res;
	*pts = resb;

	return sz;
}

void *one_request (void *qwq)
{
	int arg = (int) qwq;
	char **mov;
	double *pts;
	int size;

	double profile[NUM_OF_GENRE];
	double sum = 0;
	for (int i = 0; i < NUM_OF_GENRE; i++)
	{
		profile[i] = reqs[arg]->profile[i];
		sum += profile[i] * profile[i];
	}
	if (sum)
	{
		sum = sqrt(sum);
		for (int i = 0; i < NUM_OF_GENRE; i++)
		{
			profile[i] /= sum;
		}
	}

	if (reqs[arg]->keywords[0] == '*')
	{
		mov = malloc(sizeof(char *) * num_of_movies);
		pts = malloc(sizeof(double) * num_of_movies);
		size = num_of_movies;
		for (int i = 0; i < num_of_movies; i++)
		{
			mov[i] = malloc(MAX_LEN);
			strcpy(mov[i], movies[i]->title);
			pts[i] = score(movies[i]->profile, profile);
		}
	}
	else size = filter(&mov, &pts, reqs[arg]->keywords, profile);

	char **a = malloc(sizeof(char *) * size);
	double *b = malloc(sizeof(double) * size);

	thread_arg root;
	root.mov = mov;
	root.pts = pts;
	root.a = a;
	root.b = b;
	root.size = size;
	root.depth = 0;

	pthread_t rtid;
	if (pthread_create(&rtid, NULL, merge_sort, &root))
	{
		ERR_EXIT("can't create thread");
	}

	pthread_join(rtid, NULL);

	char fn[MAX_LEN];
	sprintf(fn, "%dt.out", reqs[arg]->id);
	FILE *flog = fopen(fn, "w");
	
	for (int i = 0; i < size; i++)
	{
		fputs(mov[i], flog);
		fputc('\n', flog);
		free(mov[i]);
	}
	free(mov), free(pts), free(a), free(b), fclose(flog);
	
	return 0;
}

int main(int argc, char *argv[]){
	FILE *fp;

	if ((fp = fopen("./data/movies.txt","r")) == NULL){
		ERR_EXIT("fopen");
	}

	initialize(fp);
	assert(fp != NULL);
	fclose(fp);	

	pthread_t tids[MAX_REQ];
	for (int i = 0; i < num_of_reqs; i++)
	{
		if (pthread_create(tids + i, NULL, one_request, (void *) i))
		{
			ERR_EXIT("can't create thread");
		}
	}

	for (int i = 0; i < num_of_reqs; i++)
	{
		pthread_join(tids[i], NULL);
	}

	return 0;
}

/**=======================================
 * You don't need to modify following code *
 * But feel free if needed.                *
 =========================================**/

request* read_request(){
	int id;
	char buf1[MAX_LEN],buf2[MAX_LEN];
	char delim[2] = ",";

	char *keywords;
	char *token, *ref_pts;
	char *ptr;
	double ret;

	scanf("%u %254s %254s",&id,buf1,buf2);
	keywords = malloc(sizeof(char)*strlen(buf1)+1);
	if(keywords == NULL){
		ERR_EXIT("malloc");
	}

	memcpy(keywords, buf1, strlen(buf1));
	keywords[strlen(buf1)] = '\0';

	double* profile = malloc(sizeof(double)*NUM_OF_GENRE);
	if(profile == NULL){
		ERR_EXIT("malloc");
	}
	ref_pts = strtok(buf2,delim);
	for(int i = 0;i < NUM_OF_GENRE;i++){
		ret = strtod(ref_pts, &ptr);
		profile[i] = ret;
		ref_pts = strtok(NULL,delim);
	}

	request* r = malloc(sizeof(request));
	if(r == NULL){
		ERR_EXIT("malloc");
	}

	r->id = id;
	r->keywords = keywords;
	r->profile = profile;

	return r;
}

/*=================initialize the dataset=================*/
void initialize(FILE* fp){

	char chunk[MAX_LEN] = {0};
	char *token,*ptr;
	double ret,sum;
	int cnt = 0;

	assert(fp != NULL);

	// first row
	if(fgets(chunk,sizeof(chunk),fp) == NULL){
		ERR_EXIT("fgets");
	}

	memset(movies,0,sizeof(movie_profile*)*MAX_MOVIES);	

	while(fgets(chunk,sizeof(chunk),fp) != NULL){
		
		assert(cnt < MAX_MOVIES);
		chunk[MAX_LEN-1] = '\0';

		const char delim1[2] = " "; 
		const char delim2[2] = "{";
		const char delim3[2] = ",";
		unsigned int movieId;
		movieId = atoi(strtok(chunk,delim1));

		// title
		token = strtok(NULL,delim2);
		char* title = malloc(sizeof(char)*strlen(token)+1);
		if(title == NULL){
			ERR_EXIT("malloc");
		}
		
		// title.strip()
		memcpy(title, token, strlen(token)-1);
	 	title[strlen(token)-1] = '\0';

		// genres
		double* profile = malloc(sizeof(double)*NUM_OF_GENRE);
		if(profile == NULL){
			ERR_EXIT("malloc");
		}

		sum = 0;
		token = strtok(NULL,delim3);
		for(int i = 0; i < NUM_OF_GENRE; i++){
			ret = strtod(token, &ptr);
			profile[i] = ret;
			sum += ret*ret;
			token = strtok(NULL,delim3);
		}

		// normalize
		sum = sqrt(sum);
		for(int i = 0; i < NUM_OF_GENRE; i++){
			if(sum == 0)
				profile[i] = 0;
			else
				profile[i] /= sum;
		}

		movie_profile* m = malloc(sizeof(movie_profile));
		if(m == NULL){
			ERR_EXIT("malloc");
		}

		m->movieId = movieId;
		m->title = title;
		m->profile = profile;

		movies[cnt++] = m;
	}
	num_of_movies = cnt;

	// request
	scanf("%d",&num_of_reqs);
	assert(num_of_reqs <= MAX_REQ);
	for(int i = 0; i < num_of_reqs; i++){
		reqs[i] = read_request();
	}
}
/*========================================================*/

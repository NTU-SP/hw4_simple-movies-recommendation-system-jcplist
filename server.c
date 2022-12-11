#include "header.h"
#ifdef PROCESS
#include <sys/mman.h>
#define peach_alloc(tt) mmap(NULL, (tt), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
#define max_depth 3
#else
#define peach_alloc(tt) malloc((tt))
#define max_depth 5
#endif

movie_profile* movies[MAX_MOVIES];
unsigned int num_of_movies = 0;
unsigned int num_of_reqs = 0;

// no edit, globally shared!
request* reqs[MAX_REQ];
// merge after split, no race condition!

void initialize(FILE* fp);
request* read_request();

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

	if (arg->size <= 1024 || arg->depth >= max_depth)
	{
#ifdef PROCESS
		char (*new_pool)[MAX_LEN] = (char (*) [MAX_LEN]) arg->mov[0];
#endif
		sort(arg->mov, arg->pts, arg->size);
#ifdef PROCESS
		for (int i = 0; i < arg->size; i++)
		{
			strcpy(new_pool[i], arg->mov[i]);
			arg->mov[i] = new_pool[i];
		}
#endif
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
#ifdef PROCESS
	pid_t lpid, rpid;
	if ((lpid = fork()) == 0)
	{
		merge_sort(&left);
		exit(0);
	}
	else if (lpid < 0)
	{
		ERR_EXIT("fork failed");
	}
	if ((rpid = fork()) == 0)
	{
		merge_sort(&right);
		exit(0);
	}
	else if (rpid < 0)
	{
		ERR_EXIT("fork failed");
	}
	while(wait(0) > 0);
#else
	pthread_t ltid;
	if (pthread_create(&ltid, NULL, merge_sort, &left))
	{
		ERR_EXIT("can't create thread");
	}

	merge_sort(&right);

	pthread_join(ltid, NULL);
#endif
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

int filter (char ***mov, double **pts, const char *key, const double *profile, char (**movie_name_pool)[MAX_LEN])
{
	int sz = 0;
	for (int i = 0; i < num_of_movies; i++)
	{
		if (strstr(movies[i]->title, key) != NULL) sz++;
	}

	char **res = peach_alloc(sizeof(char *) * sz);
	double *resb = peach_alloc(sizeof(double) * sz);
#ifdef PROCESS
	char (*resp)[MAX_LEN] = peach_alloc(MAX_LEN * sz);
#endif
	for (int i = 0, j = 0; j < sz; i++)
	{
		if (strstr(movies[i]->title, key) != NULL)
		{
#ifdef PROCESS
			res[j] = resp[j];
			strcpy(res[j], movies[i]->title);
#else
			res[j] = movies[i]->title;
#endif
			resb[j] = score(movies[i]->profile, profile);
			j++;
		}
	}
	*mov = res;
	*pts = resb;
#ifdef PROCESS
	*movie_name_pool = resp;
#endif
	return sz;
}

void *one_request (void *qwq)
{
	int arg = *(int *)qwq;
	char **mov;
	double *pts;
	int size;

	char (*movie_name_pool)[MAX_LEN] = NULL;

	if (reqs[arg]->keywords[0] == '*')
	{
		mov = peach_alloc(sizeof(char *) * num_of_movies);
		pts = peach_alloc(sizeof(double) * num_of_movies);
#ifdef PROCESS
		movie_name_pool = peach_alloc(MAX_LEN * num_of_movies);
#endif
		size = num_of_movies;
		for (int i = 0; i < num_of_movies; i++)
		{
#ifdef PROCESS
			mov[i] = movie_name_pool[i];
			strcpy(mov[i], movies[i]->title);
#else
			mov[i] = movies[i]->title;
#endif
			pts[i] = score(movies[i]->profile, reqs[arg]->profile);
		}
	}
	else size = filter(&mov, &pts, reqs[arg]->keywords, reqs[arg]->profile, &movie_name_pool);

	char **a = peach_alloc(sizeof(char *) * size);
	double *b = peach_alloc(sizeof(double) * size);

	thread_arg root;
	root.mov = mov;
	root.pts = pts;
	root.a = a;
	root.b = b;
	root.size = size;
	root.depth = 0;

#ifdef PROCESS
	pid_t rpid;
	if ((rpid = fork()) == 0)
	{
		merge_sort(&root);
		exit(0);	
	}
	else if (rpid < 0)
	{
		ERR_EXIT("fork failed");
	}
	while (wait(0) > 0);
#else
	merge_sort(&root);
#endif

	char fn[MAX_LEN];
#ifdef PROCESS
	sprintf(fn, "%dp.out", reqs[arg]->id);
#else
	sprintf(fn, "%dt.out", reqs[arg]->id);
#endif
	FILE *flog = fopen(fn, "w");
	
	for (int i = 0; i < size; i++)
	{
		fputs(mov[i], flog);
		fputc('\n', flog);
	}
	fclose(flog);
	
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

	int args[MAX_REQ];
	pthread_t tids[MAX_REQ];
	for (int i = 0; i < num_of_reqs; i++)
	{
		args[i] = i;
		if (pthread_create(tids + i, NULL, one_request, args + i))
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
 * But feel free if needed.	*
 =========================================**/

request* read_request(){
	int id;
	char buf1[MAX_LEN],buf2[MAX_LEN];
	char delim[2] = ",";

	char *keywords;
	char *ref_pts;
	char *ptr;
	double ret,sum;

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
	sum = 0;
	ref_pts = strtok(buf2,delim);
	for(int i = 0;i < NUM_OF_GENRE;i++){
		ret = strtod(ref_pts, &ptr);
		profile[i] = ret;
		sum += ret*ret;
		ref_pts = strtok(NULL,delim);
	}

	// normalize
	sum = sqrt(sum);
	for(int i = 0;i < NUM_OF_GENRE; i++){
		if(sum == 0)
				profile[i] = 0;
		else
				profile[i] /= sum;
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

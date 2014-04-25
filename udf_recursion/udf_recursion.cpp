
#include <stdio.h>
#include <my_global.h>
#include <mysql.h>
#include <my_sys.h>
#include <map>
#include <list>
#include <new>
#include <vector>
#include <algorithm>
#include <ctype.h>

#if defined(MYSQL_SERVER)
#include <m_string.h>		/* To get strmov() */
#else
/* when compiled as standalone */
#include <string.h>
#define strmov(a,b) strcpy(a,b)
#endif

#ifdef _WIN32
/* inet_aton needs winsock library */
#pragma comment(lib, "ws2_32")
#endif

#ifdef HAVE_DLOPEN

#if !defined(HAVE_GETHOSTBYADDR_R) || !defined(HAVE_SOLARIS_STYLE_GETHOST)
static pthread_mutex_t LOCK_hostname;
#endif

C_MODE_START;

my_bool start_rec_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void start_rec_deinit(UDF_INIT* initid);
void start_rec_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void start_rec_clear(UDF_INIT *initid, char *is_null, char *error);
char* start_rec(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error);

my_bool end_rec_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void end_rec_deinit(UDF_INIT* initid);
void end_rec_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void end_rec_clear(UDF_INIT *initid, char *is_null, char *error);
char* end_rec(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error);

my_bool get_level_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void get_level_deinit(UDF_INIT* initid);
longlong get_level(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error);

my_bool get_root_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void get_root_deinit(UDF_INIT* initid);
longlong get_root(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error);

my_bool get_position_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void get_position_deinit(UDF_INIT* initid);
longlong get_position(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error);

my_bool is_branch_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void is_branch_deinit(UDF_INIT* initid);
longlong is_branch(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error);

C_MODE_END;

std::map<int, int> table_ids;
std::list<int> position_id;
std::map<int, int> table_positions;

my_bool start_rec_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	// Allocation of the length of the string in char characters
	char *result = strdup(MYSQL_SERVER_VERSION);

	if (result == NULL)
	{
		strcpy(message, "start_rec() allocation error");
		return 1;
	}
	initid->max_length = strlen(MYSQL_SERVER_VERSION);
	initid->ptr = result;
	initid->const_item = 1;

	if (!(table_ids.empty()))
	{
		strcpy(message, "New recursion can't be initialized. Call end_rec() to end the previous one.");
		return 1;
	}

	if (args->arg_count != 2)
	{
		strcpy(message, "start_rec() accepts only two arguments.");
		return 1;
	}
	if (args->arg_type[0] != INT_RESULT)
	{
		strcpy(message, "Wrong arguments for recursion.");
		return 1;
	}  
	return 0;
}

void start_rec_deinit(UDF_INIT* initid)
{
	free(initid->ptr);
}

void start_rec_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	std::list<int>::iterator i;
	try {
		int primary = *((int*)args->args[0]);
		int parent = (args->args[1]) ? *((int*)args->args[1]) : 0; //set NULL-parents as "-1"
		if (primary == parent) *error = 1;
			else table_ids[primary] = parent;
		i = std::find(position_id.begin(), position_id.end(), parent);
		if (i != position_id.end()) position_id.insert(++i, primary);
		else position_id.push_back(primary);
	} catch (...) {
		table_ids.clear();
		*error = 1;
	}
}

void start_rec_clear(UDF_INIT *initid, char *is_null, char *error)
{
}

char* start_rec(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
	if (table_ids.empty())
	{
		strcpy(result, "ERROR: Recursion can't be started. Table is empty/overflowing");
		*res_length = strlen("ERROR: Recursion can't be started. Table is empty/overflowing");
		return result;
	}
	else 
	{
		if (*error == 1) 
		{		
			strcpy(result, "ERROR: Recursion can't be started. Table must not have pairs (PRIMARY key = PARENT key)");
			*res_length = strlen("ERROR: Recursion can't be started. Table must not have pairs (PRIMARY key = PARENT key)");
			return result;
		}
		else
		{
			std::list<int>::iterator i;
			int n = 1;
			for (i = position_id.begin(); i != position_id.end(); ++i)
				table_positions[(*i)] = n++;
			strcpy(result, "Recursion was initialized.");
			*res_length = strlen("Recursion was initialized");
			return result;
		}
	}
}

my_bool get_level_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{

	if (args->arg_count != 1)
	{
		strcpy(message, "get_level() accepts only one argument");
		return 1;
	}
	if (table_ids.empty())
	{
		strmov(message, "Recursion can't be started. Use start_rec() function.");
		return 1;
	}
	if (args->arg_type[0] != INT_RESULT)
	{
		strcpy(message, "The argument must be the primary key.");
		return 1;
	}
	return 0;
}

void get_level_deinit(UDF_INIT* initid)
{
	if (initid->ptr != NULL)
	free(initid->ptr);
}

longlong get_level(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* message __attribute__((unused)))
{
	int* primary = (int*) args->args[0];

	int* level = new int(1);
		while (table_ids.find((*primary)) != table_ids.end())
		{
			(*primary) = table_ids[(*primary)];
			if (table_ids.find((*primary))!=table_ids.end()) (*level)++;
		}
		return *level;
}

my_bool get_root_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	if ((args->arg_count != 1)&&(args->arg_count != 2))
	{
		strcpy(message, "get_root() accepts one or two arguments");
		return 1;
	}
	if (table_ids.empty())
	{
		strmov(message, "Recursion can't be started. Use start_rec() function.");
		return 1;
	}
	if (args->arg_type[0] != INT_RESULT)
	{
		strcpy(message, "The argument must be the primary key.");
		return 1;
	}
	return 0;
}

void get_root_deinit(UDF_INIT* initid)
{
	if (initid->ptr != NULL)
	free(initid->ptr);
}

longlong get_root(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error)
{
	
	int* primary = (int*) args->args[0];
	int* level_root= (args->args[1]) ? (int*) args->args[1] : new int(0); // needed level of the root

	int* level_current = new int(1); // actual level of the current id
	if ((*level_root) == 0)
		while (table_ids.find((*primary)) != table_ids.end())
		{
			(*primary) = table_ids[(*primary)];
		}
	else
		while ((table_ids.find((*primary)) != table_ids.end())&&((*level_current)<=(*level_root)))
		{
			(*primary) = table_ids[(*primary)];
			if (table_ids.find((*primary))!=table_ids.end()) (*level_current)++;
		}
	return *primary;
}

my_bool get_position_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	if (args->arg_count != 1)
	{
		strcpy(message, "get_position() accepts only one argument");
		return 1;
	}
	if (table_ids.empty())
	{
		strmov(message, "Recursion can't be started. Use start_rec() function.");
		return 1;
	}
	if (args->arg_type[0] != INT_RESULT)
	{
		strcpy(message, "The argument must be the primary key.");
		return 1;
	}
	return 0;
}

void get_position_deinit(UDF_INIT* initid)
{
}

longlong get_position(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error)
{
	int primary = *((int*)args->args[0]);

	return table_positions[primary];
}

my_bool is_branch_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	if (args->arg_count != 2)
	{
		strcpy(message, "is_branch() accepts two arguments");
		return 1;
	}
	if (table_ids.empty())
	{
		strmov(message, "Recursion can't be started. Use start_rec() function.");
		return 1;
	}
	if ((args->arg_type[0] != INT_RESULT)&&(args->arg_type[1] != INT_RESULT))
	{
		strcpy(message, "Arguments must be integers");
		return 1;
	}
	return 0;
}

void is_branch_deinit(UDF_INIT* initid)
{
}

longlong is_branch(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error)
{
	int* primary = (int*) args->args[0];
	int* root= (int*) args->args[1];
	int res = 0;
	if (*primary == *root) res = 1;
	else
	{
		while (table_ids.find(*primary) != table_ids.end())
		{
			(*primary) = table_ids[(*primary)];
			if (table_ids.find(*primary) == table_ids.find(*root))
			{
				res = 1;
				break;
			}
		}
	}
	return res;
}
//
///
////











my_bool end_rec_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	char *result = strdup(MYSQL_SERVER_VERSION);

	if (result == NULL)
	{
		strcpy(message, "start_rec() allocation error");
		return 1;
	}
	initid->max_length = strlen(MYSQL_SERVER_VERSION);
	initid->ptr = result;
	initid->const_item = 1;
	if (args->arg_count != 0)
	{
		strcpy(message, "end_rec() doesn't accept any arguments");
		return 1;
	}
	return 0;
}

void end_rec_deinit(UDF_INIT* initid)
{
	table_ids.clear();
	position_id.clear();
	table_positions.clear();
	if (initid->ptr != NULL)
	free(initid->ptr);
}

void end_rec_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
}
void end_rec_clear(UDF_INIT *initid, char *is_null, char *error)
{
}
char* end_rec(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
	if (table_ids.empty())
	{
		strcpy(result, "ERROR: Recursion wasn't started");
		*res_length = strlen("ERROR: Recursion wasn't started");
		return result;
	}
	else
	{
		strcpy(result, "Recursion is completed");
		*res_length = strlen("Recursion is completed");
		return result;
	}
}

#endif /* HAVE_DLOPEN */
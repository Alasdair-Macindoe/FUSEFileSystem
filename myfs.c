/*
  This Fuse file system is based largely on the HelloWorld example by Miklos
	Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html).
	Additional inspiration was taken from Joseph J. Pfeiffer's
	"Writing a FUSE Filesystem: a Tutorial"
 (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
 ***
	Which was then adapted by Alasdair Macindoe
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>

#include <errno.h>
#include <fcntl.h>

#include "myfs.h"

//We treat the root as if it was a directory, and store it here
file* root_directory;
file* requested_file;


/*
 ***************
	Helper Methods
 ***************
*/

/**
 * Find's the number in the array where this parent holds its child
 *
 * @param path the path of the file to be found
 * @param dir the parent directory (exact parent)
 *
 * @return the number in data structure that the child is located at. or a < 0
 *				 number if the child cannot be located.
 */
int find_child_number(const char* path, file* dir){
	write_log("-- Finding child number --\n");
	//We do not want compiler warnings about 'const' becoming omitted when used
	//Later and this avoids that
	char file_path[MY_MAX_PATH];
	memset(file_path,0,MY_MAX_PATH);
	strcat(file_path, path);
	write_log("File path to find: %s\n", file_path);
	write_log("Parent path: %s ID: %x\n", dir->path, dir->meta_data_id);
	int number_children = dir->number_children;
	write_log("Trying to find %s, total children to consider: %d\n", path, \
						number_children);

	//This is a simple iteration through all children in the structure to see if
	//any of them have the path we are looking for
	for (int i=REST_POS; i<number_children; i++){
			file* child = malloc(sizeof(file));
			unqlite_int64 size = sizeof(file);
			write_log("Check ID: %x\n", dir->children[i]);
			int rc = unqlite_kv_fetch(pDb,&dir->children[i], KEY_SIZE, child,&size);
			if (rc != UNQLITE_OK){
				write_log("DB error in finding child number");
				return rc;
			}
			char* fpath = (char*)&(child->path);
			write_log("Comparing %s to path: %s which is position: %d\n",path,\
			 					fpath, i);

			if(strcmp(file_path, fpath)==0) {
				write_log("File found at index: %d\n", i);
				return i;
			}
	}
	//If we exit this loop we did not find the file. Return -1.
	return -1;
}

/**
 * Retrieves the file object for a specific
	init_fs(); path.
 *
 * @param the path to find the file struct for
 *
 * @return will place the file into the requested_file cache, if not found
 * will place < 0 as the size of the current requested_file.
 */
void traverse_to_file(const char* path, uuid_t parent){
	int len = strlen(path);
	char i_path[len]; //Sometimes weird things happen as described below
	memcpy(i_path, path, len);
	i_path[len] = '\0';
	write_log("-- Traversing to File --\n");
	write_log("Internal path: %s path: %s length: %d\n",i_path, path, len);

	//Special case
	if (strcmp(path,"/")==0){
		write_log("Returning root!\n");
		unqlite_int64 size = sizeof(file);
		int rc = unqlite_kv_fetch(pDb, &root_directory->meta_data_id, KEY_SIZE, \
															requested_file, &size);
		//For this cae we need to ensure that both the cache and the root directory
		//are consistent.
		memcpy(root_directory, requested_file, sizeof(file));
		write_log("Root ID: %x\n", root_directory->meta_data_id);
		write_log("Requested file ID: %x\n", requested_file->meta_data_id);
		return;

	}else{
		//Sometimes /b can be passed in as /b/ which will skew results. This fixes
		//that occurrence.
		write_log("Char to consider: %c\n",(char)(i_path[len-1]));
		if ((i_path[len-1]) == '/') {
			write_log("Replacing character\n");
			i_path[len-1] = '\0';
		}
		write_log("New path: %s\n", i_path);

	}

	write_log("Non-root requested, parent ID: %x\n", parent);
	//We know it was not the root that was requested now
	//We want to iterate from our parent directory throughout the tree which it
	//is the head of to see if we can find our file
	file* current_file = malloc(sizeof(file));
	unqlite_int64 size = sizeof(file);
	//Because we start from the parent UUID  we do not always need to traverse
	//the entire tree
	int rc = unqlite_kv_fetch(pDb, parent, KEY_SIZE, current_file, &size);

	//Sanity check
	if (rc != UNQLITE_OK){
		write_log("DB error in traversing to file\n");
		return;
	}

	//This will hold the sub directory we are current in eg: when we split
	//a/b/c/d.txt we will b e in sub directories: a, b, c and d.txt. This holds
	//where which one we are currently on.
	char* subdir = malloc(MY_MAX_PATH);
	memset(subdir, 0, MY_MAX_PATH);

	char delim[2] = "/\0"; //Standard deliminator on UNIX systems

	//We also want to avoid warnings about const char* path with the const being
	//omitted, so we have to create our own copy.
	char internal_path[MY_MAX_PATH];
	strcpy(internal_path, i_path);

	//Split the path based on the deliminator into sections
	subdir = strtok(internal_path, delim);

	//We need to build up the directory as we go.
	//eg: for a/b/c/d.txt we need to traverse through a to find b, then b to find
	//c and finally c to find d.txt. We are unable to jump from a -> d.txt.
	char current_path[MY_MAX_PATH];
	memset(current_path,'\0', MY_MAX_PATH);

	int position; //position of current child we need to move to.

	while (subdir != NULL){
		write_log("Current token: %s \n", subdir);
		write_log("Current file path: %s\n", current_file->path);
		if (strcmp(path, current_file->path)==0){
			//We have found the file
			rc = unqlite_kv_fetch(pDb, current_file->meta_data_id, KEY_SIZE,  \
													requested_file, &size);
			write_log("Req ID: %x CF ID: %x\n", requested_file->meta_data_id, \
								current_file->meta_data_id);
			return;
		 }

		//We need to add our deliminator to our current path (/) and then add
		//the current directory we are looking at (eg a,b etc.) so we can build up
		//as we traverse through the tree
		strcat(current_path, delim);
		strcat(current_path, subdir);
		write_log("Current path: %s\n", current_path);

		position = find_child_number(current_path, current_file);

		//Safety first
		if (position < 0){
			write_log("File not found (traverse to file)\n");
			requested_file->size = -1;
			return;
		 }

		//Update and allow us to traverse further down the tree
		int rc = unqlite_kv_fetch(pDb, &(current_file->children[position]), \
															KEY_SIZE, current_file, &size);
		write_log("Found and updated current_file\n");

		if (rc != UNQLITE_OK){
			write_log("DB error in traversing to file\n");
			requested_file->size = -1;
			return;
		}

		write_log("Compare: %s to %s\n", current_file->path, i_path);
		if (strcmp(current_file->path, i_path)== 0){
			//Copy it to cache
			memcpy(requested_file, current_file, sizeof(file));
			requested_file->size = current_file->size;
			write_log("Ensuring files are sized correctly: %d\n", \
										requested_file->size);
			write_log("File found! %x Requested ID: %x\n", \
										current_file->meta_data_id, requested_file->meta_data_id);
			write_log("Cache file's path: %s\n", current_file->path);
			return;
		}

		//We haven't found the file so iteratively traverse the tree until we have
		write_log("New directory: %s\n", current_file->path);
		subdir = strtok(NULL, delim); //Split again until we cannot any longer
		write_log("Subdir: %s\n", subdir);

	}

	free(subdir); //Tidying up
	free(current_file);
	write_log("Traverse to file with UUID: %x\n", current_file->meta_data_id);
}

/**
 * Returns the path of the parent folder
 *
 * @param path the path we want to find the parent of
 * @param file_dir the buffer we want to place the parent's path into
 */
void traverse_to_folder(const char* path, char* file_dir){
	write_log("-- Attempting to find folder --\n");
	memset(file_dir, 0, MY_MAX_PATH); //Avoids weird errors
	char delim = '/';
	char* fname = strrchr(path,(int)delim);
	write_log("fname %s\n", fname);
	int length_fname = strlen(fname);
	int length_dname = strlen(path) - strlen(fname) +1; //Offset for null
	 																										//terminator
	strncpy(file_dir, path, length_dname);
	write_log("Path: %s File Name: %s Searching for: %s\n", path, fname, \
						file_dir);
}

/**
 * Loads the parent of a file into the cache
 *
 * @param child the child whose parent should be found
 *
 * @return places the parent into the cache, or makes the cache's file size < 0
 * if the file could not be located (generally this should never happen).
 */
void cache_parent(file* child){
	write_log("--Attempting to cache parent --\n");

	if(strcmp(child->children[PARENT_POS], zero_uuid)==0){
		requested_file->size = -1;
		write_log("File has no parent!\n");
		return;
	}else{
		unqlite_int64 size = sizeof(file);
		int rc = unqlite_kv_fetch(pDb, &child->children[PARENT_POS], KEY_SIZE, \
			 												requested_file, &size);
		write_log("Parent should be in cache: %s\n", requested_file->path);

		if (rc != UNQLITE_OK){
			write_log("Problem with caching parent\n");
			requested_file->size = -1;
			return;
		}

	}
}

/**
 * Deletes a child from its parent, also updates the number of children that
 * the parent has.
 *
 * @param path the path of the node to be deleted
 * @param dir the parent directory
 */
void delete_child(const char* path, file* parent){
	write_log("-- Attempting to delete a child --\n");
	int child_number = find_child_number(path, parent);

	if (child_number < 0) {
		//Hopefully this never actually happens.
		write_log("Child not found\n");
		return; //Child has not been found, so nothing to delete
	}else{
		for (int i = child_number + 1; i < parent->number_children; i++){
			memcpy(parent->children[i-1], parent->children[i], sizeof(uuid_t));
		}
	}

	parent->number_children = parent->number_children - 1;
	write_log("Parent should now have: %d children\n", parent->number_children);
}

/**
 * Attempts to cache a file for ease of use. Should minimise DB calls.
 *
 * @param path the path of the file we wish to cache
 *
 * @return 0 on success, -ENOENT on file not found
 */
int do_caching(const char* path){
	write_log("-- Attempting to cache--\n");
	//Checks to see if it is cached already making checking cache effectively
	//"free" in comparison to making a DB call.
	if (strcmp(requested_file->path, path)==0){
		write_log("%s is already cached\n", path);
		return 0; //Do nothing
	}else{
		write_log("%s is not cached\n", path);
		traverse_to_file(path, root_directory->meta_data_id);
		write_log("Attempted to traverse\n");

		//Safety first
		if (requested_file->size < 0){
			write_log("Do caching: File not found\n");
			return -ENOENT;
		}else{
			write_log("File exists at: %s and %x\n", requested_file->path, \
																								requested_file->meta_data_id);
			return 0;
		}

	}

	return -ENOENT; //should never happen
}

/**
 * Formats the path recieved to an appropriate format
 *
 * @param the path to be formatted
 *
 * @return the correctly formatted path in the path variable
 */
void format_path(char* path){
	write_log("Path recieved %s\n", path);
	char delim[2] = "/\0";
	char* new_path = malloc(MY_MAX_PATH);

	new_path = strtok(path, delim);
	write_log("New path: %d\n", new_path);

	while (new_path != NULL){
		strcpy(path, new_path);
		write_log("Copied to path: %s\n", path);
		new_path = strtok(NULL, delim);
		write_log("New path: %s", delim);
	}

	write_log("Returning: %s\n", path);
	free(new_path);
}


/*
 *************************
	Myfs System Call Methods
 *************************
*/

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
/**
 * Gets the attributes of a file (its inode)
 * @param path the path of the file
 * @param stbuf the stat struct its data has to be put into
 * @return 0 on success, non-0 on failure
 */
static int myfs_getattr(const char *path, struct stat* stbuf){
	write_log("\n== ATTEMPTING GETATTR ==\n");
	write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);
	write_log("Requested file's path: %s\n", requested_file->path);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("Getattr file not found\n");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//At this point the file we want should be in the cache
	//Set all of the data in stbuf to zero
	memset(stbuf, 0, sizeof(struct stat));
	//Then transfer all of it to the buffer
	write_log("Reading child with UUID: %x\n", requested_file->file_data_id);
	stbuf->st_mode = requested_file->mode;
	write_log("Mode (IS DIR) %d\n", (stbuf->st_mode & S_IFMT) == S_IFDIR);
	stbuf->st_nlink = requested_file->number_children - 1; //Include itself
	stbuf->st_uid = requested_file->uid;
	stbuf->st_gid = requested_file->gid;
	stbuf->st_mtime = requested_file->mtime;
	stbuf->st_ctime = requested_file->ctime;
	stbuf->st_size = requested_file->size;
	write_log("size: %d\n", requested_file->size);
	return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
/**
 * Reads a file path into the buffer
 * @param path the file path to be read into the buffer
 * @param buf the buffer where this data should be stored
 * @param filler the function that writes data into the buffer
 * @param offset the offset compared to the base address
 * @param fi information on the state of the open files
 * @return 0 on success, non-zero on failure
 */
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, \
												off_t offset, struct fuse_file_info *fi)
{
	write_log("\n== ATTEMPTING READDIR ==\n");
	//Cast our inputs to void because these parameters are not needed yet
	(void) offset;
	(void) fi;
	//Logging
	write_log("myfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, \
	offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("Getattr file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//Our file should be in the cache
	//Fill the buffer with the current directory and its parent directory
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	write_log("File found at path: %s\n", requested_file->path);
	write_log("Number of children: %d\n", requested_file->number_children);
	for (int i=REST_POS; i<(requested_file->number_children); i++){
		//Sadly we need to make DB calls for each child
		file* child = malloc(sizeof(file));
		unqlite_int64 size = sizeof(file);
		unqlite_kv_fetch(pDb, &requested_file->children[i], KEY_SIZE, child, \
										 &size);

		write_log("Child: %s\n", child->path);
		char* pathP = child->path;
		format_path(pathP);
		write_log("Pathp: %s\n", pathP);

		//Add to buffer
		if (pathP != NULL){
			write_log("Adding %s to buffer\n",pathP);
			//fill the buffer with the path
			filler(buf, pathP, NULL, 0);
		}

		free(child); //Tidying up
}

		write_log("readdir terminated \n");
		return 0;
}

// Read a file.
// Read 'man 2 read'.
/**
 * Reads a file into memory
 * @param path the file path
 * @param buf the buffer it has to be stored in.
 * @param the size of the buffer to be read into
 * @param the offset to where it should be stored in memory
 * @parm fi information on the state of the open files
 * @return the number of bytes read into memory
 */
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, \
	 										struct fuse_file_info *fi)
{
	write_log("\n== ATTEMPTING READ ==\n");
	size_t len;
	(void) fi;
	//Logging
	write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, \
	 						fi=0x%08x)\n", path, buf, size, offset, fi);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("Getattr file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);


	//The length of the file to be read
	len = requested_file->size;
	write_log("File size: %d\n", len);
	//The data block to store the file
	//It does not need to be larger than our buffer size
	uint8_t data_block[size];
	//Set all of its memory to 0
	memset(&data_block, '\0', size);
	//Get the UUID
	uuid_t* data_id = &(requested_file->file_data_id);
	write_log("File UUID: %x\n", requested_file->file_data_id);

	if(uuid_compare(zero_uuid,*data_id)!=0){
		write_log("myfs_read file with non-0 UUID\n");
		//This is similar to ORM
		unqlite_int64 nBytes;  //Data length.
		//Get the data stored at this key
		//When we have NULL we are asking to get its size back
		int rc = unqlite_kv_fetch(pDb,data_id,KEY_SIZE,NULL,&nBytes);
		write_log("Size: %d\n",nBytes);

		//Error handling
		if( rc != UNQLITE_OK ){
		  error_handler(rc);
		}

		//Fetch the fcb the root data block from the store.
		unqlite_kv_fetch(pDb,data_id,KEY_SIZE,&data_block,&nBytes);
	}

	//Place into the correct place in memory
	if (offset < len) {
		//If we can store this in memory then do so
		if (offset + size > len) {
			size = len - offset;
			memcpy(buf, &data_block + offset, size);
		}
	} else{
		size = 0;
	}

	return size;
}

/**
* Creates a file at a specified location
* @param path the path where the file should be located at
* @param the mode affiliated with this file
* @param fi information on the state of the open files
* @return 0 upon success, non-zero upon failure
*/
static int myfs_create(const char *path, mode_t mode, \
												struct fuse_file_info *fi)
{
	write_log("\n== ATTEMPTING CREATE ==\n");
  write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);

	//If the path length is too long
	int pathlen = strlen(path);
	if(pathlen>=MY_MAX_PATH){
		write_log("myfs_create - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}
	write_log("Mode: %d IS FILE: %d\n", mode, mode & S_IFMT == S_IFREG);
	//Find path excluding name
	char file_dir[MY_MAX_PATH];
	traverse_to_folder(path, file_dir); //Get us its parent's address
	write_log("Folder found %s\n",file_dir);

	//Create a new fcb for this new file
	//Update in memory data structures
	file* parent = malloc(sizeof(file));

	if (parent == NULL){
		write_log("Error mallocing!\n");
		return -1;
	}

	//Ensure we did not request the root directory
	if (strcmp(file_dir,"/")!=0){
		unqlite_int64 size = sizeof(file);
		//Its parent should be a directory and thus we want its meta data
		traverse_to_file(file_dir, root_directory->meta_data_id);

		int rc = unqlite_kv_fetch(pDb, &requested_file->meta_data_id, KEY_SIZE, \
															parent, &size);

		if (rc != UNQLITE_OK){
			write_log("DB Error in MYFS CREATE\n");
			return rc;
		}

		write_log("Parent: %s\n", parent->path);
		if (parent == NULL) {
			write_log("myfs create directory (parent) not found");
			return UNQLITE_NOTFOUND;
		}

	}else{
		//We have putting a new file in the root directory
		unqlite_int64 size = sizeof(file);
		int rc = unqlite_kv_fetch(pDb, &root_directory->meta_data_id, KEY_SIZE, \
			 												parent, &size);
		if (rc != UNQLITE_OK){
			write_log("DB Error in MYFS CREATE\n");
			return rc;
		}
	}

	file* new_file = malloc(sizeof(file));
	write_log("Written to position: %d\n", parent->number_children);
	write_log("Parent: %s\n", parent->path);
	//Copy the file's address to its FCB
	strcpy(new_file->path, path);

	//Generate it new UUIDs
	uuid_generate(new_file->file_data_id);
	uuid_generate(new_file->meta_data_id);
	write_log("Meta ID: %x\t File ID: %x\n", new_file->file_data_id, \
						new_file->meta_data_id);

	//Copy the metadata UUID to its parent.
	memcpy(parent->children[parent->number_children], new_file->meta_data_id, \
				 sizeof(uuid_t));
	parent->number_children = parent->number_children + 1;
	write_log("New file with UUID: %x\n", new_file->file_data_id);
	write_log("Parent has %d children\n", parent->number_children);
	//Context of invoking user
	struct fuse_context* context = fuse_get_context();
	new_file->uid = context->uid;
	new_file->uid = context->gid;
	new_file->size = 0;
	//Default permissions are: F, USER: R/W, GROUP: R/W, ALL: R
	new_file->mode = mode;
	new_file->number_children = REST_POS;
	new_file->mtime = time(0);
	new_file->ctime = time(0);
	parent->ctime = time(0);
	//Write the two mandatory children to it
	memcpy(new_file->children[SELF_POS], new_file->meta_data_id, sizeof(uuid_t));
	memcpy(new_file->children[PARENT_POS], parent->meta_data_id, sizeof(uuid_t));

	//Notice we are updating their META DATA.
	//wc = write child, wp = write parent, wd = write data
	int wc = unqlite_kv_store(pDb, &(new_file->meta_data_id), KEY_SIZE,\
	 													new_file, sizeof(file));
	int wp = unqlite_kv_store(pDb, &(parent->meta_data_id), KEY_SIZE, parent, \
														sizeof(file));
	//Create an entry in the DB for our data
	int wd = unqlite_kv_store(pDb, &new_file->file_data_id, KEY_SIZE, NULL, 0);
	//Same sanity checks - make sure writes to DB went through correctly
	if( wc != UNQLITE_OK || wp != UNQLITE_OK || wd != UNQLITE_OK){
		write_log("myfs_create - EIO. WC: %d WP: %d WD: %d\n",wc,wp, wd);
		return -EIO;
	}

	//Copy to cache the newly created file since we probably want to use it
	memcpy(requested_file, new_file, sizeof(file));
	write_log("File copied to cache!\n");
	//if parent is root
	if (strcmp(parent->path, "/")==0){
		write_log("Parent is root\n");
		memcpy(root_directory, parent, sizeof(file));
	}
	//Housekeeping
	free(parent);
	free(new_file);
  return 0;
}

// Set update the times (actime, modtime) for a file.
// This FS only supports modtime. (So far)
// Read 'man 2 utime'.
/**
 * Updates the time attributes of a file
 * @param path the file
 * @param ubuf the utimbuf struct with the time data
 * @return 0 on success, non-zero on failure
 */
static int myfs_utime(const char *path, struct utimbuf *ubuf){
	write_log("\n== ATTEMPTING UTIME ==\n");
  write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);
	//Find us the child if it exists

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("utime file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//Otherwise update the time
	requested_file->mtime=ubuf->modtime;
	//And then write to our DB
	// Write the fcb to the store.
  int rc = unqlite_kv_store(pDb,&(requested_file->meta_data_id),\
														KEY_SIZE,requested_file,sizeof(file));
	if( rc != UNQLITE_OK ){
		write_log("myfs_utime - EIO");
		return -EIO;
	}
  return 0;
}

// Write to a file.
// Read 'man 2 write'
/**
 * Writes data to memory.
 * @param path the paFile exists atth to be written to
 * @param buf the data to be written
 * @param size the size of the data to be written
 * @param offset the offset
 * @return the amount of bytes written to disk
 */
static int myfs_write(const char* path, const char *buf, size_t size, \
	 										off_t offset, struct fuse_file_info *fi)
{
	write_log("\n=== ATTEMPTING WRITE ===\n");
  write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, \
	fi=0x%08x)\n", path, buf, size, offset, fi);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("Getattr file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//Find us the child if it exists
	write_log("Child size at start: %d\n", requested_file->size);

	//Get us a data block and set it to 0
	uint8_t data_block[size];
	memset(&data_block, '\n', size);
	//Get us a UUID of its data
	uuid_t* data_id = &(requested_file->file_data_id);
	write_log("Path: %s\n", requested_file->path);
	write_log("Reading UUID id: %x\n",*data_id);

	// Is there a data block? ie have we written to this file before?
	if(uuid_compare(zero_uuid,*data_id)==0){
		write_log("\nFile UUID needs generating!\n\n");
		//I don't imagine that this can actually happen, nor am I convinced that
		//this will actually make it work again
		// Generate a UUID for the data block. We'll write the block itself later.
		uuid_generate(requested_file->file_data_id);
		uuid_generate(requested_file->meta_data_id);
		requested_file->size = 0;
	}else{
		write_log("File already exists\n");
		//First we will check the size of the obejct in the store to ensure that
		//we won't overflow the buffer.
		unqlite_int64 nBytes;  // Data length.
		//Get us its size currently
		int rc = unqlite_kv_fetch(pDb,data_id,KEY_SIZE,NULL,&nBytes);

		write_log("n bytes: %zu\n", nBytes);
		if( rc!=UNQLITE_OK){
			write_log("myfs_write - EIO (not found)\n");
			return -EIO;
		}
	}

	// Write the data in-memory.
  int written = snprintf(data_block, size, buf);
	write_log("Written: %d\n", written);
	write_log("Size: %d\n", size);
	if (written == 0) return 0;
	// Write the data block to the store.
	// The key is the pointer to its UUID
	int rc;

	if (offset == 0){
		write_log("Adding to start of file!\n");
		rc = unqlite_kv_store(pDb, data_id, KEY_SIZE, &data_block, size);
	}else{
		write_log("Appending to the end of a file!\n");
		rc = unqlite_kv_append(pDb, data_id, KEY_SIZE, &data_block, size);
	}

	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}

	write_log("Successfully written to DB\n");
	// Update the fcb in-memory.
	time_t now = time(NULL);
	requested_file->mtime=now;
	requested_file->ctime=now;
	requested_file->size = requested_file->size + size;
	write_log("Current size: %d\n", requested_file->size);

	//Write metadata back to DB too
	write_log("Meta data ID: %x\n", requested_file->meta_data_id);
	rc = unqlite_kv_store(pDb, &requested_file->meta_data_id, KEY_SIZE, \
												requested_file, sizeof(file));
	write_log("Successfully written meta data to DB\n");

	if (rc != UNQLITE_OK){
		write_log("DB Error in myfs write\n");
		return rc;
	}

  return size;
}

// Set the size of a file.
// Read 'man 2 truncate'.
/**
 * Makes a file a requested size smaller
 * @param path the file
 * @param newsize the new size of the file
 * @return 0 upon success, non-zero upon failure
 */
int myfs_truncate(const char *path, off_t newsize){
	write_log("\n== ATTEMPTING TRUNCATE ==\n");
  write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);
	//Mustn't be larger than is allowed in size
	if(newsize >= MY_MAX_FILE_SIZE){
		write_log("myfs_truncate - EFBIG");
		return -EFBIG;
	}

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("Getattr file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	requested_file->size = newsize;

	// Write the fcb to the store.
  int rc = unqlite_kv_store(pDb,&(requested_file->meta_data_id),KEY_SIZE, \
														requested_file,sizeof(file));

	if( rc != UNQLITE_OK ){
		write_log("myfs_write - EIO");
		return -EIO;
	}

	return 0;
}

/**
 * Update's this file's permissions.
 * @param path the path of the file
 * @param mode the new permissions
 * @return 0 upon success, non-zero upon failure
 */
int myfs_chmod(const char *path, mode_t mode){
	write_log("\n== ATTEMPTING CHMOD ==");
  write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("chmod file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	requested_file->mode = mode;
	//Write back to DB - recall we only have 1 file right now
	int rc = unqlite_kv_store(pDb, &(requested_file->meta_data_id), KEY_SIZE, \
														requested_file, sizeof(file));
	//Same sanity checks
	if( rc != UNQLITE_OK ){
		write_log("myfs_create - EIO");
		return -EIO;
	}
  return 0;
}

// Set ownership.
/**
 * Changes the ownership of a file.
 *
 * @param path where the file is located
 * @param uid the user-id of the new owner
 * @param gid the group id of the new owner
 */
int myfs_chown(const char *path, uid_t uid, gid_t gid){
	write_log("== ATTEMPTING CHOWN ==");
  write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);

	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("chown file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//Find us the child if it exists
	requested_file->uid = uid;
	requested_file->gid = gid;
	//Update the database
	int rc = unqlite_kv_store(pDb, &(requested_file->meta_data_id), KEY_SIZE, \
														requested_file, sizeof(file));
	return 0;
}

// Create a directory.
/**
 * Creates a new directory
 *
 * @param path where to locate the file at
 * @param mode the permissions for the file
 */
int myfs_mkdir(const char *path, mode_t mode){
	//The trick here is that creating an empty file and creating a directory are
	//the exact same operation but with different permissions. It isn't until
	//you do something with the file/directory that things actually differ.
	write_log("\n== ATTEMPTING MKDIR ==\n");
	write_log("myfs_mkdir: %s\n",path);
	return myfs_create(path, mode|S_IFDIR, NULL);
}

/**
 * Deletes a file.
 *
 * @param path the file to be deleted
 *
 */
int myfs_unlink(const char* path){
	write_log("\n== ATTEMPTING UNLINK ==\n");
	write_log("myfs_unlink: %s\n",path);
	//NOTICE: We do not implement symlinks in this file system.
	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("unlink file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);

	//Create space for parent
	file* parent = malloc(sizeof(file));
	if(parent == NULL) {
		write_log("myfs_unlink- malloc failed\n");
		return UNQLITE_NOTFOUND;
	}
	//Load parent into local storage
	unqlite_int64 size = sizeof(file);
	int rc = unqlite_kv_fetch(pDb, &requested_file->children[PARENT_POS], \
		 												KEY_SIZE, parent, &size);
	if (rc != UNQLITE_OK){
		write_log("DB error in myfs UNLINK\n");
		return rc;
	}
	write_log("Loaded parent with path: %s\n", parent->path);
	//Find where that child exists
	int child_number = find_child_number(path, parent);
	write_log("Child located at index: %d\n", child_number);
	if (child_number < 0 ){
		write_log("myfs unlink file not found - something has gone HORRIBLY \
							wrong\n");
		return -ENOENT;
	}
	//Delete that child
	delete_child(path, parent);
	write_log("\nParent (%s) now has %d children\n",parent->path, \
						parent->number_children);
	//Write back to DB
	int wp = unqlite_kv_store(pDb, &parent->meta_data_id, KEY_SIZE, parent, \
														sizeof(file));
	if (wp != UNQLITE_OK){
		write_log("Error writing parent back to DB\n");
		return wp;
	}

	//Delete.
	//dmd = delete meta data
	//dfd = delete file data
	int dmd = unqlite_kv_delete(pDb, &requested_file->meta_data_id, KEY_SIZE);
	int dfd = unqlite_kv_delete(pDb, &requested_file->file_data_id, KEY_SIZE);
	if (dmd != UNQLITE_OK || dfd != UNQLITE_OK){
		write_log("DMD: %d DFD: %d\n", dmd, dfd);
		return (dfd==UNQLITE_OK)?dmd:dfd;
	 }

	 write_log("Deleted child from DB\n");

	//Check if root directory needs updating
	if (strcmp(parent->path,"/")==0){
		write_log("Root also needs updated\n");
		memcpy(root_directory, parent, sizeof(file));
		//Root might be cached so we need to update that too
		memcpy(requested_file, root_directory, sizeof(file));
		write_log("Root has %d children.\n", root_directory->number_children);
	}else{
		//Ensure cache remains consistent
		memcpy(requested_file, parent, sizeof(file));
	}

	//Otherwise
	free(parent);
  return 0;
}

// Delete a directory.
/**
 * Deletes a directory.
 *
 * @param path the path of the directory
 *
 */
int myfs_rmdir(const char *path){
	//Once again, observer that deleting a file and deleting an empty directory
	//are virtually the same thing. By making our logical in unlink a tiny bit
	//more complex (doing 1 check to see if it is a file or a directory and doing
	//one change base upon it) can save us from having 100s of lines of
	//duplicated code. A discussion of why this isn't refactored into its own
	//method is in the report.
	write_log("\n==ATTEMPTING RMDIR==\n");
	write_log("myfs_rmdir: %s\n",path);
	//Attempt caching.
	if (do_caching(path) == -ENOENT){
		write_log("unlink file not found");
		return -ENOENT;
	}

	write_log("File should be cached: %s\n", requested_file->path);
	if (requested_file->size < 0){
		write_log("Directory not found\n");
		return -ENOENT;
	}

	if (requested_file->number_children > 2){
		write_log("Directory not empty\n");
		return -ENOTEMPTY;
	}

	//Otherwise remove as if it was a regular file
	return myfs_unlink(path);
}


// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
		write_log("\n==ATTEMPTING FLUSH==\n");

    write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);

    return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
		write_log("\n==ATTEMPTING RELEASE==\n");
    write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);

    return retstat;
}

// Open a file. Open should check if the operation is permitted for the given
// flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi){
	write_log("\n== ATTEMPTING OPEN ==\n");
	write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);
	write_log("Flags: %d\n", fi->flags);
	if (do_caching(path) == -ENOENT){
		write_log("unlink file not found");
		return -ENOENT;
	}
	write_log("File should be cached: %s\n", requested_file->path);
	if (requested_file->size < 0){
		write_log("Directory not found\n");
		return -ENOENT;
	}

	int mode = requested_file->mode;
	write_log("Mode: %d\n", mode);
	if ((mode & S_IRUSR) == S_IRUSR){
		write_log("Permission granted!\n");
		return 0;
	}else{
		write_log("Permission denied!\n");
		return -EACCES;
	}

	return 0;
}

static struct fuse_operations myfs_oper = {
	.getattr	= myfs_getattr,
	.readdir	= myfs_readdir,
	.open		= myfs_open,
	.read		= myfs_read,
	.create		= myfs_create,
	.utime 		= myfs_utime,
	.write		= myfs_write,
	.truncate	= myfs_truncate,
	.flush		= myfs_flush,
	.release	= myfs_release,
	.chmod = myfs_chmod,
	.chown = myfs_chown,
	.unlink = myfs_unlink,
	.rmdir = myfs_rmdir,
	.mkdir = myfs_mkdir,
	.rmdir = myfs_rmdir,
};

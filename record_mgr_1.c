//
//  record_mgr.c
//  testAsg3
//
//  Created by 平宇薛 on 11/6/16.
//  Copyright © 2016 pingyuXue. All rights reserved.
//

#include <stdio.h>


#include <string.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"   
#include "tables.h"


size_t sizeint = sizeof(int );
size_t sizechar = sizeof(char );
size_t sizebool = sizeof(bool );
size_t sizefloat = sizeof(float );




//RC initRecordManager (void *mgmtData){
//    return RC_OK;
//}
//
//
//RC shutdownRecordManager (){
//    return RC_OK;
//}
//
//
//RC createTable (char *name, Schema *schema){
//    
//    // test the create table work or not:
//    if (schema == NULL){
//        printf("schema is null,stop create\n");
////        return RC_UNESPECTED_ERROR;
//        return RC_FILE_NOT_FOUND;
//    }
//    
//    RC rc_init;  // rc_ judege
//    RC flag ;   // rc_flag
//    SM_FileHandle filehandle;
////    SM_PageHandle pagehandle;
//    
//    // data which in use;
//    char *inputMessage;  //char
//    char *CSchema;
//    
//    // the size of the table which in use
//    RC fileSize;   // fileetadatasize;
//    RC recordSize;
//    RC slotSize;
//    int recordnum;
//    
//    //get the file
//    rc_init = createPageFile(name);
//    if ( rc_init != RC_OK){
//        printf("get file error, please check name of file  line54\n");
//        return rc_init;
//    }
//    
//    openPageFile(name, &filehandle);
//    
//    // get the size of the file
//    fileSize = (int )(strlen(serializeSchema(schema)) + (4* sizeint));  // ?? 4* sizeof (int);
//    
//    if (fileSize % PAGE_SIZE == 0){
//        fileSize = fileSize / PAGE_SIZE;
//    }
//    else{
//        fileSize = ( fileSize / PAGE_SIZE ) + 1;
//    }
//    
//    // get the info. from the file and store it into the file
//    
//    slotSize = SLOT_SIZE; // ? why?
//    
//    recordSize = (getRecordSize(schema)/ (slotSize));
//    recordnum = 0;
//    
//    inputMessage = (char *)malloc(sizeof(char)  * PAGE_SIZE);
//    
//    memcpy(inputMessage, &fileSize, sizeof(int));
//    memcpy(inputMessage + sizeint, &recordSize, sizeint);
//    memcpy(inputMessage + 2 * sizeint,  &slotSize, sizeint);
//    memcpy(inputMessage + 3 * sizeint, &recordnum, sizeint);
//    
//    
//    CSchema = serializeSchema(schema);
//    
//    // check write info.
////    flag = ensureCapacity(fileSize, &filehandle);
////    if( rc_init != RC_OK){
////        rc_init = closePageFile( &filehandle);
////        return rc_init;
////    }
//    
//    ensureCapacity(fileSize, &filehandle);
//    
//    int i= 1;
//    
//    if ( strlen( CSchema) < PAGE_SIZE - (4* sizeint)) {   // pagesize- 4*sizeof(int)
//        memcpy(inputMessage + 4 * sizeint, CSchema, strlen(CSchema));
//        flag  = writeBlock(0, &filehandle, inputMessage);
//        free(inputMessage);
//    }
//    
//    else{
//        memcpy( inputMessage + 4 * sizeint, CSchema, PAGE_SIZE - 4 * sizeint);
//        flag = writeBlock(0, &filehandle, inputMessage);
//        free(inputMessage);
//        
//        while ( ++i < fileSize){
//            inputMessage = (char *) calloc(PAGE_SIZE, sizeof(char));
//            if ( i == fileSize - 1){
//                memcpy(inputMessage, CSchema + i* PAGE_SIZE, strlen(CSchema + i * PAGE_SIZE));
//            }
//            else{
//                memcpy(inputMessage, CSchema + i * PAGE_SIZE, PAGE_SIZE);
//            }
//            
//            flag = writeBlock(i, &filehandle, inputMessage);
//            free( inputMessage);
//        }
//    }
//    
//    if (flag != RC_OK){
//        flag = closePageFile(&filehandle);
//        printf("flag not rc_ok, check createTable()1 \n");
//        return flag;
//    }
//    
//    flag = addOnePageData(&filehandle);
//    if ( flag != RC_OK){
////        flag = closePageFile(&filehandle);
//        closePageFile(&filehandle);
//        printf("flag not rc_ok, check createTable() 2\n");
//        return flag;
//    }
//    
//    // close the filehandle pointer;
//    closePageFile(&filehandle);
//    
//    return RC_OK;
//    
//}
//
//
//RC openTable (RM_TableData *rel, char *name)
//{
//    RC flag = -99;
//    char * flagC1;
//    char * flagC2;
////    char * flagC3;
//    
//    int attrNum;
//    char **attrNames;  //? pointer **
//    
//    DataType *datatype;
//    
//    int *typeLength;
//    int *keyAttrs;
//    
//    int keysize;
//    
//    SM_FileHandle *filehandle = (SM_FileHandle *) malloc (sizeof(SM_FileHandle));
//    BM_BufferPool *bufferManage = MAKE_POOL();
//    BM_PageHandle *pagehandel = MAKE_PAGE_HANDLE();
//    
//    Schema *schema;
//    
//    int fileDataSize = -1;
//    char * CSchema;
//    
//    char * tempChar;
//    
//    // check the init infromation ;
//    flag = openPageFile(name, filehandle);
//    if (flag != RC_OK){
//        printf("open page file error,  in openTable() 1 \n");
//        return flag;
//    }
//    
//    flag = initBufferPool(bufferManage, name, 10, RS_LRU, NULL);
//    if(flag != RC_OK){
//        printf( "initBufferPool wrong, in openTable() 2\n");
//        return flag;
//    }
//    
//    fileDataSize = getFileDataSize(bufferManage);
//    
//    int i = 0;
//    
//    // change while (i) to while (i++)    _1641
//    while (i < fileDataSize){
//        pinPage(bufferManage, pagehandel, i);
//        unpinPage(bufferManage, pagehandel);
//        i++;
//    }
//    
//    // test:
//    printf("cschema :%s\n",CSchema);
//    printf("pagehadle data : %s\n", pagehandel->data);
//    
//    CSchema=(pagehandel->data)+ (4 * sizeof(int));  // ？ 没有传入值- fixed ,
//    
//    printf("cschema :%s\n",CSchema);
//    
//    flagC1 = strchr(CSchema, '<');
//    flagC1++;
//    
//
//    
//    
//    tempChar= (char *) malloc ( sizechar * 40);
//    
////    int j = 1; // change test1
//    int j = 0;
//    
//    while (1){
//        if( *(flagC1 + j) == '>'){
//            break;
//        }
//        
//        tempChar[j] = flagC1[j];
//        j++;
//    }
//    
//    attrNum = atoi(tempChar);
//    free(tempChar);
//    
//    
//    
//    // not understand
//    flagC1 = strchr(flagC1, '(');
//    flagC1 ++;
//    
//    attrNames = (char **) malloc ( attrNum * sizeof (char *));
//    datatype = (DataType *) malloc ( attrNum * sizeof(DataType));
//    typeLength = (int *) malloc( attrNum * sizeof(int));
//    
//    int k ; // i
//    int m = 0;     // j
//    int kk;
//    
//    for (k = 0;k< attrNum; ++k){
////        while (1){
//        for ( m = 0; 1; ++m){
//        
//            if ( *(flagC1 + m) == ':'){
//                attrNames[k] = (char *) malloc( m * sizeof(char));
//                memcpy(attrNames[k], flagC1, m);
//               
//                
//                switch( *(flagC1 + m + 2)){   // why locate here?
//                    case 'I':
//                        datatype[k] = DT_INT;
//                        typeLength[k] = 0;
//                        break;
//                    case 'F':
//                        datatype[k] = DT_FLOAT;
//                        typeLength[k] = 0;
//                        break;
//                    case 'S':
//                        datatype[k] = DT_STRING;
//                        tempChar = (char*)malloc(sizeof(char) *40);
//                        
////                        while (1){
//                        for ( kk = 0; 1; ++kk){
//                            if ( *(flagC1 + kk + m + 9) == ']'){
//                                break;
//                            }
//                            
//                            tempChar[kk] = flagC1 [kk + m + 9];
//                        }
//                        
//                        typeLength[k] = atoi(tempChar);
//                        
//                        free((tempChar));
//                        break;
//                        
//                    case 'B':
//                        datatype[k] = DT_BOOL;
//                        typeLength[k] = 0;
//                        break;
//                }
//                
//                if ( k == (attrNum -1)){
//                    break;
//                }
//                
//                flagC1 = strchr(flagC1, ',');
//                flagC1 = flagC1 +2;
//                
//                break;
//            }
//        }
//    }
//    
//    flagC1 = strchr(flagC1, '(');
//    flagC1 ++;
//    flagC2 = flagC1;
//    
//    int ii = 1;
//    while (1){
//        flagC2 = strchr(flagC2, ',');
//        if (flagC2 == NULL) {
//            break;
//        }
//        flagC2 ++;
//        ii++;
//    }
//    
//    keysize = ii + 1;
//
//    keyAttrs = (int *)malloc (keysize * sizeint);
//    
//    int jj; //i
//    int mm ; //j  //add 0756
//    int z;  //k
//    
//    for( jj =0; jj<keysize; ++jj){
////        while (1){
//        for ( mm=0 ; 1; ++mm){
//            if ( (*(flagC1 + mm) == ',') ||  (*(flagC1 +mm) == ')')){
//                tempChar = (char *) malloc (100 * sizechar);
//                
//                memcpy(tempChar, flagC1, mm);
//                
//                for ( z=0; z<attrNum;++z ){
//                    if (strcmp(tempChar, attrNames[z]) == 0 ){
//                        keyAttrs[jj] = z;
//                        free(tempChar);
//                        break;
//                    }
//                }
//                
//                if ( *(flagC1 + mm) == ','){
//                    flagC1 = flagC1 + mm + 2;
//                }
//                else {
//                    flagC1 = flagC1 + mm;
//                }
//                break;
//            }
////            mm ++;  // add 0756
//        }
//    }
//    
//    //init the schema
//    schema = createSchema(attrNum, attrNames, datatype, typeLength, keysize, keyAttrs);
//    
//    
//    // init rel
//    rel->name = name;
//    rel->bm = bufferManage;
//    rel->fh = filehandle;
//    rel->schema = schema;
//    
//    return RC_OK;
//    
//}



/*
 file meta data :
 file meta data size(int)
 record size (int)
 slot size (int)
 struct Schema:
 
 page meta data:
 page number (int)
 capabiltiy (int)
 
 
 */


RC initRecordManager (void *mgmtData){
    return RC_OK;
}


RC shutdownRecordManager (){
    return RC_OK;
}


extern RC createTable (char *name, Schema *schema){
    
    //    // test the create table work or not:
    //    if (schema == NULL){
    //        printf("schema is null,stop create\n");
    //        return RC_UNESPECTED_ERROR;
    //    }
    //
    //    RC rc_init;  // rc_ judege
    //    RC flag ;   // rc_flag
    //    SM_FileHandle filehandle;
    ////    SM_PageHandle pagehandle;
    //
    //    // data which in use;
    //    char *inputMessage;  //char
    //    char *CSchema;
    //
    //    // the size of the table which in use
    //    RC fileSize;   // fileetadatasize;
    //    RC recordSize;
    //    RC slotSize;
    //    int recordnum;
    //
    //    //get the file
    //    rc_init = createPageFile(name);
    //    if ( rc_init != RC_OK){
    //        printf("get file error, please check name of file  line54\n");
    //        return rc_init;
    //    }
    //
    //    openPageFile(name, &filehandle);
    //
    //    // get the size of the file
    //    fileSize = (int )(strlen(serializeSchema(schema)) + (4* sizeint));  // ?? 4* sizeof (int);
    //
    //    if (fileSize % PAGE_SIZE == 0){
    //        fileSize = fileSize / PAGE_SIZE;
    //    }
    //    else{
    //        fileSize = ( fileSize / PAGE_SIZE ) + 1;
    //    }
    //
    //    // get the info. from the file and store it into the file
    //
    //    slotSize = SLOT_SIZE; // ? why?
    //
    //    recordSize = (getRecordSize(schema)/ (slotSize));
    //    recordnum = 0;
    //
    //    inputMessage = (char *)malloc(sizeof(char)  * PAGE_SIZE);
    //
    //    memcpy(inputMessage, &fileSize, sizeof(int));
    //    memcpy(inputMessage + sizeint, &recordSize, sizeint);
    //    memcpy(inputMessage + 2 * sizeint,  &slotSize, sizeint);
    //    memcpy(inputMessage + 3 * sizeint, &recordnum, sizeint);
    //
    //
    //    CSchema = serializeSchema(schema);
    //
    //    // check write info.
    ////    flag = ensureCapacity(fileSize, &filehandle);
    ////    if( rc_init != RC_OK){
    ////        rc_init = closePageFile( &filehandle);
    ////        return rc_init;
    ////    }
    //
    //    ensureCapacity(fileSize, &filehandle);
    //
    //    int i= 1;
    //
    //    if ( strlen( CSchema) < PAGE_SIZE - (4* sizeint)) {   // pagesize- 4*sizeof(int)
    //        memcmp(inputMessage + 4 * sizeint, CSchema, strlen(CSchema));
    //        flag  = writeBlock(0, &filehandle, inputMessage);
    //        free(inputMessage);
    //    }
    //
    //    else{
    //        memcmp( inputMessage + 4 * sizeint, CSchema, PAGE_SIZE - 4 * sizeint);
    //        flag = writeBlock(0, &filehandle, inputMessage);
    //        free(inputMessage);
    //
    //        while ( ++i < fileSize){
    //            inputMessage = (char *) calloc(PAGE_SIZE, sizeof(char));
    //            if ( i == fileSize - 1){
    //                memcmp(inputMessage, CSchema + i* PAGE_SIZE, strlen(CSchema + i * PAGE_SIZE));
    //            }
    //            else{
    //                memcmp(inputMessage, CSchema + i * PAGE_SIZE, PAGE_SIZE);
    //            }
    //
    //            flag = writeBlock(i, &filehandle, inputMessage);
    //            free( inputMessage);
    //        }
    //    }
    //
    //    if (flag != RC_OK){
    //        flag = closePageFile(&filehandle);
    //        printf("flag not rc_ok, check createTable()1 \n");
    //        return flag;
    //    }
    //
    //    flag = addOnePageData(&filehandle);
    //    if ( flag != RC_OK){
    ////        flag = closePageFile(&filehandle);
    //        closePageFile(&filehandle);
    //        printf("flag not rc_ok, check createTable() 2\n");
    //        return flag;
    //    }
    //
    //    // close the filehandle pointer;
    //    closePageFile(&filehandle);
    //
    //    return RC_OK;
    
    RC flag =- 99;
    
    if( name == NULL || schema == NULL){
        printf("null input, error\n");
        return flag;
    }
    
    
    SM_FileHandle filehandle;  //Q1: why not char *?
    
    // file_meta_data;
    int slot_size;
    int record_num;
    int record_size;
    
    int get_file_storage_size; // get the size of the schema and load the extra size
    int file_meta_data_size; //file meta data size
    
    
    // transfer :
    char * input_infor = (char *) malloc (sizechar * PAGE_SIZE);
    char * schema_data = serializeSchema(schema);
    
    int writeposition = 1;
    
    
    // create pagefile
    createPageFile(name);
    openPageFile(name, &filehandle);
    
    //     get informatino for the filemeta data
    //     file_meta_data has the lenth of the schema's attr
    //        and also have the record_num, record_size, slot_size in the contents
    //        its for the meta contrl
    
    record_num = 0;
    slot_size = SLOT_SIZE;
    record_size = getRecordSize(schema) / slot_size ;
    
    get_file_storage_size = strlen(serializeSchema(schema)) + 4* sizeint;

    if (get_file_storage_size % PAGE_SIZE == 0){
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE);
    }
    else
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE) + 1;
    
    ensureCapacity(file_meta_data_size, &filehandle);
    
    // transfer data into input_infor and write into block
    memcpy(input_infor, &file_meta_data_size, sizeint);
    memcpy(input_infor + sizeint, &record_size, sizeint);
    memcpy(input_infor + (2 * sizeint), &slot_size, sizeint);
    memcpy(input_infor + (3 * sizeint), &record_num, sizeint);
    
    // get schema infor and input into the
    //** file_meta_data_tr and extra_size is make easy to read
    
    int extra_size = (4 * sizeint);
    int file_meta_data_tr = PAGE_SIZE - extra_size;  // PAGE_SIZE - 4 * sizeint
    
    
    // scheam data is less than the page size or not
    if (strlen(schema_data) < file_meta_data_tr){
        memcpy(input_infor + extra_size , schema_data, strlen(schema_data));
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
    }
    
    else {
        memcpy(input_infor + extra_size, schema_data, file_meta_data_tr);
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
        
        // reload input:
        for (; writeposition < file_meta_data_size; writeposition++){
            // calloc 0 page of the
            input_infor = (char *) calloc (PAGE_SIZE, sizeint);
            
            if ( writeposition != (file_meta_data_size - 1)){
                memcpy(input_infor, schema_data + writeposition * PAGE_SIZE, PAGE_SIZE);
            }
            // last page input
            else {
                memcpy(input_infor, schema_data + writeposition * PAGE_SIZE, strlen(schema_data + writeposition * PAGE_SIZE));
            }
            writeBlock(writeposition, &filehandle, input_infor);
            free(input_infor);
        }
        
    }
    Page_Meta_Data(&filehandle);
    
    // create table done
    closePageFile(&filehandle);
    
    return  RC_OK;
}


RC openTable (RM_TableData *rel, char *name)
{
    //    RC flag = -99;
    //    char * flagC1;
    //    char * flagC2;
    ////    char * flagC3;
    //
    //    int attrNum;
    //    char **attrNames;
    //
    //    DataType *datatype;
    //    int *typeLength;
    //    int *keyAttrs;
    //
    //    int keysize;
    //
    //    SM_FileHandle *filehandle = (SM_FileHandle *) malloc (sizeof(SM_FileHandle));
    //    BM_BufferPool *bufferManage = MAKE_POOL();
    //    BM_PageHandle *pagehandel = MAKE_PAGE_HANDLE();
    //
    //    Schema *schema;
    //
    //    int fileDataSize = -1;
    //    char *CSchema;
    //
    //    // check the init infromation ;
    //    flag = openPageFile(name, filehandle);
    //    if (flag != RC_OK){
    //        printf("open page file error,  in openTable() 1 \n");
    //        return flag;
    //    }
    //
    //    flag = initBufferPool(bufferManage, name, 10, RS_LRU, NULL);
    //    if(flag != RC_OK){
    //        printf( "initBufferPool wrong, in openTable() 2\n");
    //        return flag;
    //    }
    //
    //    fileDataSize = getFileDataSize(bufferManage);
    //
    //    int i = 0;
    //
    //    while (i < fileDataSize){
    //        pinPage(bufferManage, pagehandel, i);
    //        unpinPage(bufferManage, pagehandel);
    //        i++;
    //    }
    //
    //    CSchema = pagehandel->data + ( 4 * sizeint );
    //
    //    flagC1 = strchr(CSchema, '<');
    //    flagC1++;
    //
    //    char * tempChar;
    //
    //
    //    tempChar= (char *) malloc ( sizechar * 40);
    //    int j = 1;
    //    while (1){
    //        if( *(flagC1 + j) == '>'){
    //            break;
    //        }
    //
    //        tempChar[j] = flagC1[j];
    //        j++;
    //    }
    //
    //    attrNum = atoi(tempChar);
    //    free(tempChar);
    //
    //    flagC1 = strchr(flagC1, '(');
    //    flagC1 ++;
    //
    //    attrNames = (char **) malloc ( attrNum * sizeof (char *));
    //    datatype = (DataType *) malloc ( attrNum * sizeof(DataType));
    //    typeLength = (int *) malloc( attrNum * sizeof(int));
    //
    //    int k ; // i
    //    int m = 0;     // j
    //    int kk;
    //
    //    for (k = 0;k< attrNum;k++){
    //        while (1){
    //
    //            if ( *(flagC1 + m) == ':'){
    //                attrNames[k] = (char *) malloc( m * sizeof(char));
    //                memcpy(attrNames[k], flagC1, m);
    //
    //
    //                switch( *(flagC1 + m + 2)){
    //                    case 'I':
    //                        datatype[k] = DT_INT;
    //                        typeLength[k] = 0; break;
    //                    case 'F':
    //                        datatype[k] = DT_FLOAT;
    //                        typeLength[k] = 0; break;
    //                    case 'S':
    //                        datatype[k] = DT_STRING;
    //                        tempChar = (char*)malloc(sizeof(char) *40);
    //
    //                        while (1){
    //                            if ( *(flagC1 + kk + m + 9) == ']'){
    //                                break;
    //                            }
    //
    //                            tempChar[kk] = flagC1 [kk + m + 9];
    //                        }
    //
    //                        typeLength[k] = atoi(tempChar);
    //
    //                        free((tempChar));
    //                        break;
    //
    //                    default: datatype[k] = DT_BOOL, typeLength[k] = 0;
    //                }
    //
    //                if ( k == attrNum -1){
    //                    break;
    //                }
    //
    //                flagC1 = strchr(flagC1, ',');
    //                flagC1 = flagC1 +2;
    //
    //                break;
    //            }
    //        }
    //    }
    //
    //    flagC1 = strchr(flagC1, '(');
    //    flagC1 ++;
    //    flagC2 = flagC1;
    //
    //    int ii = 1;
    //    while (1){
    //        flagC2 = strchr(flagC2, ',');
    //        if (flagC2 == NULL) {
    //            break;
    //        }
    //        flagC2 ++;
    //        ii++;
    //    }
    //    keysize = ii + 1;
    //
    //    keyAttrs = (int *)malloc (keysize * sizeof(int));
    //
    //    int jj; //i
    //    int mm; //j
    //    int z;  //k
    //
    //    for( jj =0; jj<keysize; ++jj){
    //        while (1){
    //
    //            if ( (*(flagC1 + mm) == ',') ||  (*(flagC1 +mm) == ')')){
    //                tempChar = (char *) malloc (100 * sizeof(char));
    //                memcpy(tempChar, flagC1, mm);
    //
    //                for ( z=0; z<attrNum;++z ){
    //                    if (strcmp(tempChar, attrNames[z]) ==0 ){
    //                        keyAttrs[jj] = z;
    //                        free(tempChar);
    //                        break;
    //                    }
    //                }
    //
    //                if ( *(flagC1 + mm) == ','){
    //                    flagC1 = flagC1 + mm + 2;
    //                }
    //                else {
    //                    flagC1 = flagC1 + mm;
    //                }
    //                break;
    //            }
    //        }
    //    }
    //
    //    //init the schema
    //    schema = createSchema(attrNum, attrNames, datatype, typeLength, keysize, keyAttrs);
    //
    //
    //    // init rel
    //    rel->name = name;
    //    rel->bm = bufferManage;
    //    rel->fh = filehandle;
    //    rel->schema = schema;
    //
    //    return RC_OK;
    
    // cr means create
    RC rc = -99;
    if( rel == NULL || name == NULL){
        printf("input error \n");
        return rc;
    }
    
    Schema *scheam_cr;
    int numAttr_cr;
    char ** attrNames_cr;
    DataType *dataTypes_cr;
    int *typeLength_cr;
    int *keyAttrs_cr;
    int keysize_cr;
    
    // create bufferpool pagehandle
    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    BM_BufferPool *bufferpool = MAKE_POOL();
    SM_FileHandle *filehandle = (SM_FileHandle *) malloc ( sizeof(SM_FileHandle));
    
    // filemetadata size & schema_data
    int file_meta_data_size;
    char * schema_data;
    
    
    // flag for input:
    char *flag;
    char *trans = (char *) calloc(40, sizechar); // temp transfor data;
    
    
    // init buffer pool
    openPageFile(name, filehandle);
    initBufferPool(bufferpool, name, 10, RS_LRU, NULL);
    file_meta_data_size = File_Meta_Data_Size(bufferpool);
    
    // get char schema data:
    int i;
    for (i=0; i< file_meta_data_size; ++i){
        pinPage(bufferpool, pagehandle, i);
        unpinPage(bufferpool, pagehandle);
    }
    
    schema_data = pagehandle->data + (4 *sizeint);
    
    
    flag = strchr(schema_data, '<');
    flag++;
    
    
    // base on the test rm_serializer file
    // get the numAttr number:
    for(i=0;;++i){
        
        if ( *(flag + i) == '>'){
            break;
        }
        
        trans[i] = flag[i];
    }
    
    numAttr_cr = atoi(trans);
    free(trans);
    
    //give the value of the other attribute in the schema:
    attrNames_cr = (char **) malloc( numAttr_cr * sizeof(char *));
    dataTypes_cr = (DataType *) malloc (numAttr_cr * sizeof(DataType));
    typeLength_cr = (int *) malloc (numAttr_cr * sizeint);
    
    // flag move on:
    flag = strchr(flag, '(');
    flag++;
    
    int j,k;
    
    for(i=0; i< numAttr_cr; ++i){
        for (j=0;;++j){
            
            if ( *(flag + j) == ':'){
                attrNames_cr[i] = (char *) malloc (j * sizechar);
                memcpy(attrNames_cr[i], flag, j);
                
                switch ( *(flag + j + 2)) {
                    case 'I':
                        dataTypes_cr[i] = DT_INT;
                        typeLength_cr[i] = 0;
                        break;
                    case 'B':
                        dataTypes_cr[i] = DT_BOOL;
                        typeLength_cr[i] = 0;
                        break;
                    case 'F':
                        dataTypes_cr[i] = DT_FLOAT;
                        typeLength_cr[i] = 0;
                        break;
                    case 'S':
                        dataTypes_cr[i]= DT_STRING;
                        
                        // coun the value
                        trans = (char *)malloc (40 * sizechar);
                        
                        for (k=0;;k++){
                            // not consider;
                            if ( *(flag + k + 9) == ']'){ // ?
                                break;
                            }
                            trans[k] = flag [k +j + 9]; //?
                        }
                        
                        typeLength_cr [i] = atoi(trans);
                        free(trans);
                        break;
                }
                if ( i == numAttr_cr -1){
                    break;
                }
                
                flag = strchr(flag, ',');
                flag += 2;
                break;
            }
        }
    }
    
    flag = strchr(flag,'(');
    flag ++;
    
    char * flag2;
    flag2 = flag;
    
    for (i= 0;;i++){
        flag2 = strchr(flag2, ',');
        
        if (flag2 == NULL){
            break;
        }
        
        flag2++;
    }
    
    keysize_cr = i+1; // i
    keyAttrs_cr = (int *) malloc(keysize_cr * sizeint);
    
    for (i =0; i< keysize_cr;++i){
        for (j=0;;++j){
            if ((*(flag + j) == ',') || (*(flag + j) == ')')) {
                trans = (char *)malloc(100 * sizechar);
                memcpy(trans, flag, j);
                for (k = 0; k < numAttr_cr; k++) {
                    
                    if (strcmp(trans,attrNames_cr[k]) == 0) {
                        keyAttrs_cr[i] = k; // assign keyAttrs
                        free(trans);
                        break;
                    }
                }
                if (*(flag + j) == ',') {
                    flag = flag + j + 2;
                }
                else {
                    flag = flag + j;
                }
                break;
            }
            
        }
    }
    
    scheam_cr = createSchema(numAttr_cr, attrNames_cr, dataTypes_cr, typeLength_cr, keysize_cr, keyAttrs_cr);
    
    rel->name = name;
    rel->bm = bufferpool;
    rel->fh = filehandle;
    rel->schema = scheam_cr;
    
    //    free(pagehandle);
    
    
    return RC_OK;
}

RC closeTable (RM_TableData *rel){
    RC flag = -99;
//    

//    shutdownBufferPool(rel->bm);
    
    rel->name = NULL;
//    rel->schema = NULL;
//    rel->mgmtData = NULL;
    
    shutdownBufferPool(rel->bm);
    
    free(rel->bm);
    free(rel->fh);
    freeSchema(rel->schema);
    
    flag = RC_OK;
    
    return flag;
    
}

RC deleteTable (char *name){
    
    destroyPageFile(name);
    
    return RC_OK;
}


int getNumTuples (RM_TableData *rel){
    
    int numTuples = getTotalPage(rel);
    
    return numTuples;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    
    BM_PageHandle *bmPageHandle = MAKE_PAGE_HANDLE();  //* h
    
    int recordSize = getRecordSize(rel->schema);  // r_size
    int fielDatasize = File_Meta_Data_Size(rel->bm);  // p_mata_index
    int recordSlotNum = (int) ((recordSize  + sizebool)/ SLOT_SIZE ) + 1;
    
    int spaceOffset = 0;
    int recordCurrNum; //r_current_num
    int numOfTum;  // numTuples.
    
    do{
        pinPage(rel->bm, bmPageHandle, fielDatasize);
        
        memcpy(&fielDatasize, bmPageHandle->data + PAGE_SIZE - sizeint , sizeint);
        
        if (fielDatasize != -1){
            unpinPage(rel->bm, bmPageHandle);
        }
        else{
            break;
        }
    }while(1);
    // change to do while
    
    // test here
    do{
        memcpy(&recordCurrNum, bmPageHandle + spaceOffset + sizeint,  sizeint);
        spaceOffset += 2 * sizeint ;
        
    }while(recordCurrNum == (PAGE_SIZE/SLOT_SIZE));
    
    // not understand
    if ( recordCurrNum == -1){
        if (spaceOffset == PAGE_SIZE){
            memcpy(bmPageHandle + PAGE_SIZE - sizeint, &rel->fh->totalNumPages, sizeint);
            Page_Meta_Data(rel->fh);
            
            markDirty(rel->bm, bmPageHandle);
            unpinPage(rel->bm, bmPageHandle);
            
            pinPage(rel->bm, bmPageHandle, rel->fh->totalNumPages);
            spaceOffset = (int) (2 * sizeint);
        }
        
        memcpy(bmPageHandle->data - 2*sizeint, &rel->fh->totalNumPages, sizeint);
        appendEmptyBlock(rel->fh);
        recordCurrNum = 0;
    }
    
    memcpy(&record->id.page, bmPageHandle->data + spaceOffset - 2 * sizeint,  sizeint);
    record->id.page = recordCurrNum * recordSlotNum;
    recordCurrNum ++;
    
    memcpy(bmPageHandle->data + spaceOffset - sizeint,  &recordCurrNum, sizeint);
    markDirty(rel->bm, bmPageHandle);
    unpinPage(rel->bm, bmPageHandle);
    
    pinPage(rel->bm, bmPageHandle, 0);
    memcpy(&numOfTum, bmPageHandle->data + 3* sizeint, sizeint);
    numOfTum ++;
    memcpy(&numOfTum, bmPageHandle->data + 3*sizeint, sizeint);
    
    markDirty(rel->bm, bmPageHandle);
    unpinPage(rel->bm, bmPageHandle);
    
    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
    
    BM_PageHandle *pagehandle = (BM_PageHandle *) malloc (sizeof(BM_PageHandle));
    
    RC flag = -99;
    
    int spaceoffset = 256* id.slot;
    
    int record_size = getRecordSize(rel->schema);
    int numtup;
    char *nullstring = (char *)malloc ((sizebool + record_size ) * sizechar);
    
    flag = pinPage(rel->bm, pagehandle, id.page);
    if( flag != RC_OK){
        printf ("check pinpage in deletepage() 1 \n");
        return flag;
    }
    memcpy(pagehandle->data +spaceoffset , nullstring, sizebool+record_size);
    
    markDirty(rel->bm, pagehandle);
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
    if (flag != RC_OK){
        printf("check unpinpage in DELETEpage() 1 \n");
    }
    
    flag = pinPage(rel->bm, pagehandle, 0);
//    if(flag != RC_OK){}
    
    memcpy(&numtup, pagehandle->data + 3 *sizeint, sizeint);
    
    numtup -= 1;

    memcpy(pagehandle->data + 3* sizeint, &numtup, sizeint);
    markDirty(rel->bm, pagehandle);
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
//    if (flag != RC_OK){}
    
    free(nullstring);
    free(pagehandle);
    
    return RC_OK;
    
}




RC updateRecord (RM_TableData *rel, Record *record){
    
    BM_PageHandle *pagehandle = (BM_PageHandle *)malloc (sizeof(BM_PageHandle));
    
    RC flag = -99;
    int record_size = getRecordSize(rel->schema);
    
    flag = pinPage(rel->bm, pagehandle, record->id.page);
    if (flag != RC_OK){
        printf("check pinpage in updateRecord()\n");
        return flag;
    }
    
    markDirty(rel->bm, pagehandle);
    memcpy(pagehandle->data + SLOT_SIZE * record->id.slot + sizebool, record->data, record_size);
    
    
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
    
    if( flag != RC_OK){
        printf("check unpinpage in UpdateRecord\n");
    }
    
    free(pagehandle);
    
    return RC_OK;
    
}


RC getRecord (RM_TableData *rel, RID id, Record *record){
    
    RC flag = -99;
    
    BM_PageHandle *bmPageHandle = (BM_PageHandle *) malloc(sizeof( BM_PageHandle));
    
    int spaceOffset = SLOT_SIZE * id.slot;
    
    int record_size = getRecordSize(rel->schema);
    
    record->id = id;
    
    flag = pinPage(rel->bm, bmPageHandle, id.page);
    
    if (flag != RC_OK){
        printf("wrong pin, check pinpage in getRecord\n");
        return flag;
    }
    
    record->data = (char *)malloc (record_size);
    memcpy(record->data, bmPageHandle->data + spaceOffset, record_size );
    //memcpy(record->data, bmPageHandle->data + spaceOffset + sizebool, record_size );
    
    record->data[record_size] = '\0';
    flag = -99;
    flag = unpinPage(rel->bm, bmPageHandle);
    
    if (flag != RC_OK){
        printf("check unpingpage in getRecord() \n");
        return flag;
    }
    
    free(bmPageHandle);
    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    scan->expr = cond;
    scan->rel = rel;
    scan->curPage = 0;
    scan->curSlot = 0;
    
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record){
    RC flag = -99;
    int counttime, count; // counttime to deley
    
    BM_BufferPool *bufferpool; // tmpbm;
    bufferpool = scan->rel->bm;
    
    int index,maxslot;
    
    int rpage;// rapge = ridpage
//    int rc;
    
    int recordsize; //trs
    
    BM_PageHandle * bmPageHandle;
    Record *recordp;  //tmp
    Value *valueresult;  //result
    
    bmPageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    recordp = (Record*)malloc (sizeof(Record));
    valueresult= (Value *)malloc (sizeof(Value));
    
    RID rid;
    
    
    recordsize = (getRecordSize(scan->rel->schema) + sizeof(bool))/ SLOT_SIZE + 1;
    index = File_Meta_Data_Size(bufferpool);
    
    pinPage(bufferpool, bmPageHandle, index);
    
    int i;
    if (scan->curPage < 0){
        printf("wrong scan curpage, in next()\n");
    }
    
    for (; scan->curPage<=index; scan->curPage++){
        memcpy(&rpage, bmPageHandle->data + (scan->curPage) * 2 * sizeof(int), sizeof(int));
        memcpy(&maxslot, bmPageHandle->data + ((scan->curPage) * 2 + 1) * sizeof(int), sizeof(int));
        
        if ( maxslot != 1){
            
            
             i = scan->curSlot;
            
            while (i < maxslot){
                
                rid.page = rpage;
                rid.slot = i * recordsize;
                
                flag = getRecord(scan->rel, rid, recordp);
                if (flag == RC_OK){
                    evalExpr(recordp, scan->rel->schema, scan->expr, &valueresult);
                    
                    if (valueresult->v.boolV){
                        record->id.page = rid.page;
                        record->id.slot = rid.slot;
                        record->data = recordp->data;
                        
                        // if near the end;
                        if (i == (maxslot -1)){
                            scan->curPage +=1;
                            scan->curSlot = 0;
                        }
                        else {
                            scan->curSlot +=1;
                        }
                        
                        free(valueresult);
                        free(recordp);
                        unpinPage(bufferpool, bmPageHandle);
                        free(bmPageHandle);
                    }
                }
                i++;
            }
        }
        else{
            printf("here is wrong 1 , in next()\n");
            unpinPage(bufferpool, bmPageHandle);
            
            for (counttime = 0;counttime < 1000; counttime++){
                count *= 2;
            }
            
            free(bmPageHandle);
//            return RC_UNESPECTED_ERROR;
            return RC_FILE_NOT_FOUND;
        }
    }
    
    printf("unknow error, in next()");
    unpinPage(bufferpool, bmPageHandle);
    
    for (counttime = 0;counttime < 1000; counttime++){
        count *= 2;
    }
    
    free(bmPageHandle);
//    return RC_UNESPECTED_ERROR;
    return RC_FILE_NOT_FOUND;
}

RC closeScan (RM_ScanHandle *scan){
    
    scan->mgmtData = NULL;
    scan->rel = NULL;
    
//    free(scan);
    
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema){
    int recordsize = 0;
    
    
    int i;
    
    for (i = 0; i< schema->numAttr; i++){
        if( schema->dataTypes[i] == DT_INT){
            recordsize += sizeint;
//            break;
        }
        else if (schema->dataTypes[i] == DT_FLOAT){
            recordsize += sizefloat;
//            break;
        }
        else if (schema->dataTypes[i] == DT_BOOL){
            recordsize += sizebool;
//            break;
        }
        else{
            recordsize += schema->typeLength[i];
//            break;
        }
    }
    
    return recordsize;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schemap = (Schema*) malloc (sizeof(Schema));
    
    schemap->numAttr = numAttr;
    schemap->attrNames = attrNames;
    schemap->dataTypes = dataTypes;
    schemap->typeLength = typeLength;
    schemap->keyAttrs = keys;
    schemap->keySize = keySize;
    
    return schemap;
}


RC freeSchema (Schema *schema){
    
//    free(schema->numAttr);
    free(schema->keyAttrs);
    free(schema->dataTypes);
    free(schema->typeLength);
    schema->keySize = -1;

    
    int i=0;
    for (; i<schema->numAttr;i++){
        free(schema->attrNames[i]);
    }
    
    schema->numAttr = -1;
    
    free(schema->attrNames);
    free(schema);
    
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
    
    *record = (Record *)malloc(sizeof(Record));
    
    (*record)->data = (char *) malloc (getRecordSize(schema) * sizechar);
    
    return RC_OK;
}

RC freeRecord (Record *record){
    free (record-> data);
//    record->data = NULL;
    
    free(record);
    
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
    int attrOffset = 0;
    
    *value = (Value *)malloc (sizeof(Value));
    
    int i;
    
    for (i = 0;i<attrNum; i++){
        if ( schema->dataTypes[i] == DT_INT){
            attrOffset += sizeint;
        }
        
        else if( schema->dataTypes[i] == DT_BOOL){
            attrOffset += sizebool;
        }
        
        else if (schema->dataTypes[i] == DT_FLOAT){
            attrOffset += sizefloat;
        }
        else{
            attrOffset += schema->typeLength[i];
        }
    }
    
    (*value)->dt = schema->dataTypes[attrNum];
    
    if (schema->dataTypes [attrNum] == DT_INT){
        memcpy( &((*value)->v.intV), record->data + attrOffset, sizeint);
    }
    
    else if (schema->dataTypes[attrNum] == DT_BOOL){
        memcpy(&((*value)->v.boolV), record->data + attrOffset, sizebool);
    }
    
    else if (schema->dataTypes[attrNum] == DT_FLOAT){
        memcpy(&((*value)->v.floatV), record->data + attrOffset, sizefloat);
    }
    
    else{
        (*value)->v.stringV = (char *)malloc ( schema->typeLength[attrNum] +1);
        memcpy((*value)->v.stringV, record->data + attrOffset, schema->typeLength[attrNum]);
        (*value)->v.stringV[attrNum +1] = '\0';
    }
    
    return RC_OK;
}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int attrOffset = 0;
    int i;
    
    for (i = 0; i< attrNum; i++){
        
        if (schema->dataTypes[i] == DT_INT){
            attrOffset += sizeint;
        }
        else if( schema->dataTypes[i] == DT_BOOL){
            attrOffset += sizebool;
        }
        
        else if( schema->dataTypes[i] == DT_FLOAT){
            attrOffset += sizefloat;
        }
        
        else {
            attrOffset += schema->typeLength[i];
        }
    }
    

        
        if (schema->dataTypes[i] == DT_INT){
            memcpy(record->data + attrOffset,  &(value->v.intV), sizeint);
        }
                   
        else if( schema->dataTypes[i] == DT_BOOL){
            memcpy(record->data + attrOffset, &(value->v.boolV), sizebool);
        }
        
        else if( schema->dataTypes[i] == DT_FLOAT){
            memcpy(record->data + attrOffset, &(value->v.floatV), sizefloat);
        }
        
        else {
            memcpy(record->data + attrOffset, &(value->v.stringV), schema->typeLength[attrNum]);
        }
    
    
    return RC_OK;
}


// additional function
RC Page_Meta_Data(SM_FileHandle *fh){
    
    RC flag = -99;
    
//    RC pagenum = -1;
//    RC pagedataNum = -1;
//    RC pagedataCapacity = -1;
    
    int page_num;
    int page_data_num;
    int page_capacity;
    
    
    char * input;
    
//    int i = 0 ;
    int i ;
    
    flag = appendEmptyBlock(fh);
    if( flag != RC_OK){
        printf(" flag not rc_ok, check addPageData, appendEmptyBlock(), in addOnePageData() \n");
        closePageFile(fh);
        return flag;
    }
    
    size_t init_size = 2 * sizeof(int);
    
    page_data_num = PAGE_SIZE/ init_size;
    input = (char *) malloc (PAGE_SIZE * sizeof(char));
    page_num = fh->totalNumPages;
    
    //    pagedataCapacity = -1;
    
//    while ( ++i < page_data_num){
    for(i = 0; i< page_data_num; i++){
        memcmp(input + (i * init_size), &page_num, sizeof(int));
        memcmp(input + (i * init_size) + sizeof(int) , &page_capacity, sizeof(int));
        
        page_num++;
        
        if ( i == page_data_num -1){
            page_num = fh->totalNumPages -1;
        }
    }
    
    flag = writeBlock(fh->totalNumPages -1, fh, input);
    if( flag != RC_OK){
        printf("the writeBlock is wrong, in addOnePageData()\n");
        return flag;
    }
    
    
    free( input);
    return  RC_OK;
    
}


extern RC File_Meta_Data_Size(BM_BufferPool *bm){
    
    int file_data_size;
    
//    BM_PageHandle *pagehandle = (BM_PageHandle *)malloc (sizeof(BM_PageHandle));
    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    RC flag = -99;
    
    pinPage(bm, pagehandle, 0);
    memcpy(&file_data_size, pagehandle->data, sizeint);
    
    unpinPage(bm, pagehandle);
    free(pagehandle);
    
    return file_data_size;
}

//extern RC recordDetection (BM_BufferPool *bm){
//    BM_PageHandle *pagehandle = (BM_PageHandle *)malloc (sizeof(BM_PageHandle));
//    
//    RC flag = - 99;
//    
//    int recordsize;
//    
//    
//    flag = pinPage(bm, pagehandle, 0);
//    memcpy(&recordsize, pagehandle->data + sizeint, sizeint);
//    
//    unpinPage(bm, pagehandle);
//    free(pagehandle);
//    
//    return recordsize;
//    
//}

int getTotalPage( RM_TableData *rel){
   
    RC flag = -99;
    int numTumples;
    
    BM_PageHandle *bmPageHandle;
    bmPageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    flag = pinPage(rel->bm, bmPageHandle, 0);
    
    if ( flag != RC_OK){
        printf(" wrong, in getTotalPage() \n");
        return flag;
    }
    
    memcmp(&numTumples, bmPageHandle->data + ( 3 * sizeof(int)), sizeof(int));
    unpinPage(rel->bm, bmPageHandle);
    
    free(bmPageHandle);
    
    return numTumples;
    
}











/* mqreply - Demo program to process and reply to requests via IBM MQ
 * 
 * Summary:
 * mqreply recieves requests on DEV.Q1 from machine S1558 consisting of a 64KB payload of MQUINT32 values.
 * The request payloads are sorted and returned to remote queue definition DEV.Q2 via transmission 
 * queue S1558.TRANS.QUE over channel E6410.S1558 
 * 
 * MQ Configuration:
 * +IBM MQ Advanced for Developers on ubuntu machine E6410
 *   -Queue Manager:
 *     -QM_E6410
 * 
 *   -Queues:
 *     -DEV.Q1          - Local queue
 *     -DEV.Q2          - Remote queue definition
 *     -S1558.TRANS.QUE - Transmission queue
 * 
 *   -Channels:
 *     -DEV.APP.SVRCONN   - Server-connection
 *     -QM_S1558.QM_E6410 - Receiver
 *     -QM_E6410.QM_S1558 - Sender
 * 
 * ----------------------------------------------------------------------------------------------
 * Date       Author        Description
 * ----------------------------------------------------------------------------------------------
 * 11/04/23   A. Hout       Original source
 * ---------------------------------------------------------------------------------------------- 
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cmqc.h>

#define BUFSIZE 16384                              //16384 * sizeof(unit32_t) = 65536 = 64KB payload

int cmp_uints(const void *, const void *);         //Compare function for qsort()

/*-------------------------Begin mainline processing-----------------------------*/
int main(int argc, char **argv)
{
   //MQ structures
   MQCNO   cnxOpt = {MQCNO_DEFAULT};               //Connection options  
   MQCSP   cnxSec = {MQCSP_DEFAULT};               //Security parameters
   MQMD    msgDsc = {MQMD_DEFAULT};                //Message Descriptor
   MQOD    reqDsc = {MQOD_DEFAULT};                //Request object Descriptor
   MQOD    rpyDsc = {MQOD_DEFAULT};                //Reply object Descriptor
   MQPMO   putOpt = {MQPMO_DEFAULT};               //Put message options
   MQGMO   getOpt = {MQGMO_DEFAULT};               //Get message options
   
   //MQ handles and variables
   MQHCONN  hCnx;                                  //Connection handle
   MQHOBJ   hReq;                                  //Request queue object handle 
   MQHOBJ   hRpy;                                  //Reply queue object handle 
   MQLONG   opnOpt;                                //MQOPEN options 
   MQLONG   clsOpt;                                //MQCLOSE options 
   MQLONG   opnCde;                                //MQOPEN completion code 
   MQLONG   resCde;                                //Reason code  
   MQLONG   cmpCde;                                //MQCONNX completion code 
   MQLONG   msglen;                                //Message length received
   MQUINT32 msgBuf[BUFSIZE];                       //Message data buffer
   
	//Connection literals/variables
   char *pQmg = "QM_E6410";                        //Target queue manager
   char *pReq = "DEV.Q1";                          //Request queue
   char *pRpy = "DEV.Q2";                          //Reply queue - remote definition
   char uid[10];                                   //User ID
   char pwd[10];                                   //User password
   FILE *pFP;                                      //Credentials file pointer
   int  bufLen;                                    //Size of reply buffer                                    
   int msgCnt=0;                                   //Count of requests processed
   
   //-------------------------------------------------------
   //Pull credentials needed to connect to the queue mgr
   //-------------------------------------------------------
   cnxOpt.SecurityParmsPtr = &cnxSec;
   cnxOpt.Version = MQCNO_VERSION_5;
   cnxSec.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
   
   pFP = fopen("/home/adam/mqusers","r");
   if (pFP == NULL){
	   fprintf(stderr, "fopen() failed in file %s at line # %d", __FILE__,__LINE__);
	   return EXIT_FAILURE;
	}
   
   int scnt = fscanf(pFP,"%s %s",uid,pwd);
	fclose(pFP);
   if (scnt < 2){
      puts("Error pulling user credentials");
      return EXIT_FAILURE;
   }
   
   //-------------------------------------------------------
   //Connect to queue manager QM_E6410
   //-------------------------------------------------------
   cnxSec.CSPUserIdPtr = uid;                                            
   cnxSec.CSPUserIdLength = strlen(uid);
   cnxSec.CSPPasswordPtr = pwd;
   cnxSec.CSPPasswordLength = strlen(pwd);
   MQCONNX(pQmg,&cnxOpt,&hCnx,&cmpCde,&resCde);                            
   
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("unable to open queue manager: %s\n",pQmg);
      MQDISC(&hCnx,&cmpCde,&resCde);
      return((int)opnCde);
   }
   
   //-------------------------------------------------------
   //Open queue DEV.Q1 for input - Request queue
   //-------------------------------------------------------
   opnOpt = MQOO_INPUT_EXCLUSIVE | MQOO_FAIL_IF_QUIESCING;
   strncpy(reqDsc.ObjectName,pReq,strlen(pReq)+1);                                                   
   MQOPEN(hCnx,&reqDsc,opnOpt,&hReq,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("unable to open %s queue for output\n",pReq);
      printf("Disconnecting from %s\n",pQmg);
      MQDISC(&hCnx,&cmpCde,&resCde);
      return((int)opnCde);
   }
   
   //-------------------------------------------------------
   //Open DEV.Q2 for output - Reply queue - Remote
   //-------------------------------------------------------
   opnOpt = MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING;
   strncpy(rpyDsc.ObjectName,pRpy,strlen(pRpy)+1);                                        
   MQOPEN(hCnx,&rpyDsc,opnOpt,&hRpy,&opnCde,&resCde);
          
   if (resCde != MQRC_NONE)
      printf("MQOPEN ended with reason code %d\n",resCde);

   if (opnCde == MQCC_FAILED){
      printf("Unable to open %s queue for output\n",pRpy);
      printf("Disconnecting from %s and exiting\n",pQmg);
      MQDISC(&hCnx,&cmpCde,&resCde);
      return (int)opnCde;
   }
   
   //-------------------------------------------------------
   //-Set get message options for the request queue - DEV.Q1
   //-Set put message options for the reply queue - DEV.Q2
   //-------------------------------------------------------
   getOpt.Version = MQGMO_VERSION_2;                                       //Don't update msg and corrl ID's
   getOpt.MatchOptions = MQMO_NONE;
   getOpt.Options = MQGMO_WAIT | MQGMO_NO_SYNCPOINT; ;
   getOpt.WaitInterval = 10000;                                            //Wait up to 10 sec for a request
   
   putOpt.Options = MQPMO_NO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING;
   putOpt.Options |= MQPMO_NEW_MSG_ID;                                     //Unique MQMD.MsgId for each request
   putOpt.Options |= MQPMO_NEW_CORREL_ID;
   
   //-------------------------------------------------------
   //Display resources in use
   //-------------------------------------------------------
   printf("\n\n\n");
   printf("Queue Manager: %s\n",pQmg);
   printf("Request Queue: %s\n",pReq);
   printf("Reply Queue:   %s\n",pRpy);
   printf("Trans Queue:   E6410.TRANS.QUE\n");
   printf("Rec Channel:   S1558.E6410\n");
   printf("Send Channel:  E6410.S1558\n\n");
   
   //-------------------------------------------------------
   //1. Retrieve request messages on DEV.Q1
   //2. Sort the payload contents - 64KB array of MQUINTS32
   //3. Reply with the results on DEV.Q2
   //-------------------------------------------------------
   do{
      bufLen = sizeof(msgBuf);
      MQGET(hCnx,hReq,&msgDsc,&getOpt,bufLen,msgBuf,&msglen,&cmpCde,&resCde);
      if (resCde != MQRC_NONE){
         if (resCde == MQRC_NO_MSG_AVAILABLE){
            puts("\nNo messages on the queue");
            break;
         }
      }
      
      qsort(msgBuf,BUFSIZE,sizeof(MQUINT32),cmp_uints);  
   
      msgDsc.MsgType = MQMT_REPLY;
      strncpy(msgDsc.ReplyToQ,pRpy,MQ_Q_NAME_LENGTH);                      
      strncpy(rpyDsc.ObjectQMgrName,msgDsc.ReplyToQ,MQ_Q_MGR_NAME_LENGTH);
      MQPUT(hCnx,hRpy,&msgDsc,&putOpt,bufLen,msgBuf,&cmpCde,&resCde);
      if (resCde != MQRC_NONE)
         printf("\nMQPUT ended with reason code %d\n",resCde);
         
      if ((++msgCnt) % 25 == 0){
         printf("\rRequests recieved: %6d   Data Processed: %.2lfMB",
                               msgCnt,(double)msgCnt * 65536 / 1048576);
         fflush(stdout);
      } 
   }while(cmpCde != MQCC_FAILED && resCde == MQRC_NONE);
   
   //-------------------------------------------------------
   //Close DEV.Q1, DEV.Q2 and QM_E6410
   //-------------------------------------------------------
   clsOpt = MQCO_NONE;
   MQCLOSE(hCnx,&hReq,clsOpt,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQCLOSE ended with reason code %d\n",resCde);
      
   MQCLOSE(hCnx,&hRpy,clsOpt,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQCLOSE ended with reason code %d\n",resCde);
     
   //Disconnect from the queue manager
   MQDISC(&hCnx,&cmpCde,&resCde);
   if (resCde != MQRC_NONE)
      printf("MQDISC ended with reason code %d\n",resCde);
   
   
	return EXIT_SUCCESS;
}


/*------------------------------Functions--------------------------------*/

//qsort() compare function for MQUINT32 values
int cmp_uints(const void *a, const void *b){
   
   const MQUINT32 *uia = (const MQUINT32 *) a;
   const MQUINT32 *uib = (const MQUINT32 *) b;
   
   return (*uia > *uib) - (*uia < *uib);
}

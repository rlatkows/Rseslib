/*
 * Copyright (C) 2002 - 2025 The Rseslib Contributors
 * 
 *  This file is part of Rseslib.
 *
 *  Rseslib is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Rseslib is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package rseslib.simplegrid.manager;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import rseslib.simplegrid.common.Communication;

/**
 * @author Rafal Latkowski
 *
 */

public class Manager implements Runnable
{    
    int[] m_aUDPPorts;
    DatagramSocket[] m_aUDPSockets;
    int m_nThreadToken;
    /* m_nThreadToken used assign unique UDP Port and to identify the thread that is currently running */

    ArrayList<String> m_aResultFiles;
    /* Array of result file names to store job results in dedicated file */
    
    HashMap<String,Integer> m_hmAssignedJobs = new HashMap<String,Integer>();
    /* m_hmAssignedJobs Contains map of assigned jobs where key is job line as a string and value is integer number of the script file  */
    HashMap<String,Integer> m_hmScheduledJobs = new HashMap<String,Integer>();
    /* m_hmAssignedJobs Contains map of currently scheduled jobs (i.e. not computed yet)  */
    HashSet<String> m_hsComputedJobs = new HashSet<String>();
    /* m_hsComputedJobs Contains map of computed jobs */
    HashMap<String,Long> m_hmActivityStatistics = new HashMap<String,Long>();

    int m_nFinishSleep = 10*60*1000;
    
    public Manager(ArrayList<String> aScriptFiles,ArrayList<String> aResultFiles)
    /*
     * Constructor of the Manager class.
     * First, potential result files are read (if they exist) to populate m_hsComputedJobs HashSet of computed jobs.
     * Than reads all script files and fills the m_hmAssignedJobs HashMap with jobs to be processed.
     * Finally, it starts a thread for each UDP port defined in rseslib.simplegrid.common.Communication.s_aUDPPorts array.
     * 
     * @param aScriptFiles List of script files to be processed.
     * @param aResultFiles List of result files where computed results will be saved.
     */
    {
        /* Copy the list of UDP ports from common place */
        m_aUDPPorts = new int[Communication.s_aUDPPorts.length];
        System.arraycopy(Communication.s_aUDPPorts, 0, m_aUDPPorts, 0, m_aUDPPorts.length);

        /* Read script files and initialize m_hsComputedJobs and m_hmAssignedJobs*/
        m_aResultFiles=aResultFiles;        
        readComputedResults(aResultFiles);
        readAssigmentScripts(aScriptFiles);
        
        /* Start the threads */
        m_aUDPSockets = new DatagramSocket[m_aUDPPorts.length];
        for (int i=0;i<m_aUDPPorts.length;i++)
        {
            m_nThreadToken = i;
            (new Thread(this)).start();
            do
            {
               try { Thread.sleep(100); } catch (Exception e) {}
            } while (m_aUDPSockets[i]==null && m_aUDPPorts[i]>0);
        }
    }
        

    void readAssigmentScripts(ArrayList<String> aScriptFiles)
    /*
     * Reads all script files and fills the scheduling queue with jobs i.e. into m_hmAssignedJobs HashMap.
     * This is done only once, when the manager is started.
     * This method is called from the constructor.
     * @param aScriptFiles List of script files to be processed.
     */
    {   
        for (int i=0;i<aScriptFiles.size();i++)
        {
            String scriptFile = aScriptFiles.get(i);
            try
            {
                BufferedReader br = new BufferedReader(new FileReader(scriptFile));
                String line;
                while (null!=(line=br.readLine()))
                {
                    if (!m_hsComputedJobs.contains(line))
                    {
                        m_hmAssignedJobs.put(line,i);
                    }
                }
                br.close();
            }
            catch (Exception e) 
            {
                System.err.println("Error reading job assignment file "+scriptFile); 
                e.printStackTrace(); 
            }
        }
    }

    void readComputedResults(ArrayList<String> aResultFiles)
    /*
     * Reads all result files and fills the computed jobs set, i.e. into m_hsComputedJobs HashSet.
     * This is done only once, when the manager is started.
     * This method is called from the constructor.
     * @param aResultFiles List of result files where computed results are stored.
     * If the result file does not exist, it is ignored.
     */
    {   
        for (int i=0;i<aResultFiles.size();i++)
        {
            String scriptFile = aResultFiles.get(i);
            try
            {
                BufferedReader br = new BufferedReader(new FileReader(scriptFile));
                String line;
                while (null!=(line=br.readLine()))
                {
                    StringTokenizer strtok = new StringTokenizer(line,"|");
                    String task = strtok.nextToken().trim(); 
                    m_hsComputedJobs.add(task);
                }
                br.close();
            }
            catch (Exception e) 
            {
                System.err.println("No results file" +scriptFile+" found."); 
            }   
        }
    }

    
    synchronized String scheduleNextLine(int my_token) throws IOException
    /*
     * Schedules next line for processing.
     * This method is called when a worker node requests a task.
     * Implementation changed to get jobs line by line and not in bulk.
     * @param my_token Unique token of the thread (m_nThreadToken) that is requesting a task.
     */
    {
        while (m_hmAssignedJobs.size()>0)
        {
            Map.Entry<String,Integer> entry = m_hmAssignedJobs.entrySet().iterator().next();
            String job_line = entry.getKey();
            int file_number = entry.getValue();

            m_hmAssignedJobs.remove(job_line);
            if (m_hsComputedJobs.contains(job_line))
            {
                System.out.println("UThread"+my_token+" job "+job_line+" already computed - skipping.");
                continue;
            }
            else
            {
                m_hmScheduledJobs.put(job_line, file_number);
                return job_line;
            }
        }
        if (m_hmScheduledJobs.size()>0)
        {
            System.out.println("UThread"+my_token+" no more jobs to schedule, recycling scheduled jobs.");
            Map.Entry<String,Integer> entry = m_hmScheduledJobs.entrySet().iterator().next();
            String job_line = entry.getKey();
            return job_line;
        }
        else
        {
            System.out.println("UThread"+my_token+" no more jobs to schedule, all jobs computed.");
            return null;
        }
    }
    
    synchronized void returnNextTask(String line) throws IOException
    /*
     * Saves returned task result to the result file.
     * Line is expected to be in the format:
     * <task>|<result>
     *  @param line Line containing task and result, separated by a pipe character '|'.
     */
    {
        StringTokenizer strtok = new StringTokenizer(line,"|");
        String task = strtok.nextToken().trim();
        if (m_hmScheduledJobs.containsKey(task))
        {
            int file = m_hmScheduledJobs.get(task);
            m_hmScheduledJobs.remove(task);
            if (!m_hsComputedJobs.contains(task))
            {
                PrintWriter pw = new PrintWriter(new FileWriter(m_aResultFiles.get(file),true));
                pw.println(line);
                pw.flush();
                pw.close();
            }
            m_hsComputedJobs.add(task);
        }
    }
    
    public void run()
    /*
     * This method is the main loop of the thread.
     * It initializes the UDP socket on the assigned port and listens for incoming datagrams as described in rseslib.simplegrid.common.Communication. 
     */
    {
        int my_token = m_nThreadToken;
        try
        {
            DatagramSocket dsoc = new DatagramSocket(m_aUDPPorts[my_token]);
            m_aUDPSockets[my_token]=dsoc;
            System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" initialized (on UDP Port "+m_aUDPPorts[my_token]+").");
            while (true)
            {
                System.out.println(Communication.getTimeStamp()+" "+getActivityStatistics());
                byte[] buf = new byte[Communication.s_nBufferSize];
                DatagramPacket dtgrm = new DatagramPacket(buf,buf.length);
                dsoc.receive(dtgrm);
                m_hmActivityStatistics.put(dtgrm.getAddress().getHostAddress(), System.currentTimeMillis());
                byte buf2[] = new byte[dtgrm.getLength()];
                System.arraycopy(buf,dtgrm.getOffset(),buf2,0,dtgrm.getLength());
                String line=new String(buf2);
                if (line.length()>22)
                    System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" received "+line.substring(0,20)+"... "+dtgrm.getAddress().getHostAddress());
                else
                    System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" received "+line+" "+dtgrm.getAddress().getHostAddress());
                if (line.equalsIgnoreCase(Communication.s_strHello))
                {
                    line = Communication.s_strWelcome;
                }
                else if (line.equalsIgnoreCase(Communication.s_strGetTask))
                {
                    line = scheduleNextLine(my_token);
                    
                }
                else if (line.startsWith(Communication.s_strPutTask))
                {
                    returnNextTask(line.substring(Communication.s_strPutTask.length()).trim());
                    line = Communication.s_strPutAnswer;
                }
                else 
                {
                    line = Communication.s_strErrorUnknownCommand;
                }
                if (line!=null)
                {
                    buf = line.getBytes();
                    dtgrm = new DatagramPacket(buf,buf.length,dtgrm.getAddress(),dtgrm.getPort());
                    dsoc.send(dtgrm);
                    if (line.length()>26)
                        System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" sent "+line.substring(0,24)+"...");
                    else
                        System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" sent "+line);
                }
                else
                {
                    if (m_hmScheduledJobs.size()==0 && m_hmAssignedJobs.size()==0)
                    {                        
                        System.out.println("UThread"+my_token+" no more scheduled job - terminating thread. Other threads wait for jobs in execution. If you sure all jobs are done you can kill the server now.");
                        break;
                    }
                    else
                    {
                        Thread.sleep(1000);
                    }
                }
            }
            dsoc.close();
            System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" run() terminated.");
        }
        catch (Exception e)
        {
            System.out.println(Communication.getTimeStamp()+" UThread"+my_token+" failed (on UDP Port "+m_aUDPPorts[my_token]+").");
            m_aUDPPorts[my_token] = -1; 
            e.printStackTrace();
        }
        
    }
    
    public static final long s_lOneMinute=1000*60;
    public static final long s_lFifteenMinutes=s_lOneMinute*15;
    public static final long s_lOneHour=s_lOneMinute*60;
    
    public String getActivityStatistics()
    {
        long time = System.currentTimeMillis();
        int cnt_1m=0;
        int cnt_15m=0;
        int cnt_1h=0;
        int cnt_total=0;
        
        for (Entry<String,Long> e :  m_hmActivityStatistics.entrySet())
        {
            cnt_total++;
            long timediff = time-e.getValue();
            if (timediff<=s_lOneHour)
            {
                cnt_1h++;
                if (timediff<=s_lFifteenMinutes)
                {
                    cnt_15m++;
                    if (timediff<=s_lOneMinute) cnt_1m++;
                }
            }
        }
        return "Node activity: "+cnt_1m+" (1m), "+cnt_15m+" (15m), "+cnt_1h+" (1h), "+cnt_total+" (tot), Jobs in Execution: "+m_hmScheduledJobs.size()+" Assigned for scheduling: "+m_hmAssignedJobs.size()+" Computed: "+m_hsComputedJobs.size();
    }
}

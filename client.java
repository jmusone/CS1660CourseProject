import java.util.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class client
{

	//Ftamrs
	private static JFrame baseFrame;
	private static JFrame actionFrame;
	private static JFrame termSearchFrame;
	private static JFrame topNSearchFrame;
	private static JFrame termDisplayFrame;
	private static JFrame topNDisplayFrame;

	//Buttons
	private static JButton constructInvertedIndiciesButton;
	private static JButton findTermButton;
	private static JButton topNButton;
	private static JButton termSearchButton;
	private static JButton topNSearchButton;
	private static JButton returnButton = new JButton("Back to Search");

	//GCP vars
	private static String region = "us-central1";
	private static String clusterName = "course-project-cluster";
	private static String endPoint = region + "-dataproc.googleapis.com:443";
	private static String iiMainClass = "invertedIndex";
	private static String tnMainClass = "topN";
	private static String projectID = "cloudcomputingcourseproject";
	private static JobControllerSettings jcc;
	private static ClusterControllerSettings ccc;
	private static HadoopJob iiJob;
	private static HadoopJob tnJob;

	//Other
	private static File[] files;
	private static File iiCompleteStorage;
	private static String searchTerm = "qqqq";
	private static String searchOutput = "";
	private static String topNOutput = "";
	private static int nVal = 10;

    public static Job waitForJobCompletion(JobControllerClient jobControllerClient, String projectId, String region, String jobId) 
    {
        
        while (true) 
        {

			Job jobInfo = jobControllerClient.getJob(projectID, region, jobId);
            
			switch (jobInfo.getStatus().getState()) 
			{

				case DONE:
					//return jobInfo;

				case CANCELLED:
					//return jobInfo;

                case ERROR:
					return jobInfo;

				default:
                    
					try 
					{

						TimeUnit.SECONDS.sleep(1);

					} 
					catch (InterruptedException e) 
					{
                    
						throw new RuntimeException(e);
                    
					}
			}
		}
	}

	public static void main(String[] args)
	{

		//This was used to test if docker worked without the GUI running
		//System.out.println("SHAAAAARK");

		try
		{

			iiCompleteStorage = new File("invertedIndexStorage.txt");
			iiCompleteStorage.createNewFile();

		}
		catch(Exception e)
		{

			System.out.println("Could not create new file, search will not work");
		}

		ccc = ClusterControllerSettings.newBuilder().setEndpoint(endPoint).build();
		jcc = JobControllerSettings.newBuilder().setEndpoint(endPoint).build();
		ccc = ClusterControllerSettings.create(ccc);
		jcc = JobControllerSettings.create(jcc);

		baseGUI();

	}

	private static void baseGUI()
	{

		baseFrame = new JFrame("Jacob Musone's Search Engine");
		baseFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		baseFrame.setSize(500, 500);

		JLabel text = new JLabel("Load My Engine");
		constructInvertedIndiciesButton = new JButton("Construct Inverted Indicies");
		JPanel panel = new JPanel();
		panel.add(text);

		panel.add(constructInvertedIndiciesButton);
		constructInvertedIndiciesButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){intoActionGUI();}});

		baseFrame.getContentPane().add(BorderLayout.CENTER, panel);
		baseFrame.setVisible(true);

	}

	private static void intoActionGUI()
	{

		try
		{

			JobPlacement iiJobPlace = JobPlacement.newBuilder().setClusterName(clusterName).build();
			iiJob = HadoopJob.newBuilder().setMainClass(iiMainClass).addJarFileUris("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/JAR/invertedindex.jar").addArgs("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/Data").addArgs("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/iiOutput").build();
			Job job = Job.newBuilder().setPlacement(iiJobPlace).setHadoopJob(iiJob).build();
			Job request = jobControllerClient.submitJob(projectID, region, job);
			String id = request.getReference().getJobId();
			CompletableFuture<Job> futureFinishedJob = CompletableFuture.supplyAsync(new Supplier<Job>(){@Override public Job get(){return waitForJobCompletion(jobControllerClient, projectID, region, id);}});
			
			int timeLimit = 10;

			try
			{

				Job jobInfo = futureFinishedJob.get(timeLimit, TimeUnit.MINUTES);
				System.out.println("Job " + id + "finished");

				Cluster cInfo = ccc.getCluster(projectID, region, clusterName);
				Storage iiStorage = StorageOptions.getDefaultInstance().getService();
				Blob blob = iiStorage.get(cInfo.getConfig().getConfigBucket(),"Output/part-r-00000");
				iiCompleteStorage.write(new String(blob.getContent()));	

			}
			catch(TimeoutException toEx)
			{

				System.out.println("Job timed out");

			}

		}
		catch(Exception e)
		{

			System.out.println("FAILURE");
			System.out.println(e);

			if(jcc != null)
			{

				jcc.close();
			}

		}

		actionGUI();

	}

	private static void actionGUI()
	{

		actionFrame = new JFrame("Jacob Musone's Search Engine");
		actionFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		actionFrame.setSize(500, 500);

		JLabel text = new JLabel("Inverted indicies created successfully!");
		findTermButton = new JButton("Search for term");
		topNButton = new JButton("Get Top-N");
		JPanel panel = new JPanel();
		panel.add(text);

		panel.add(findTermButton);
		findTermButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){termSearchGUI();}});

		panel.add(topNButton);
		topNButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){topNSearchGUI();}});

		actionFrame.getContentPane().add(BorderLayout.CENTER, panel);
		baseFrame.setVisible(false);
		actionFrame.setVisible(true);

	}

	private static void termSearchGUI()
	{

		termSearchFrame = new JFrame("Jacob Musone's Search Engine");
		termSearchFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		termSearchFrame.setSize(500, 500);

		JLabel text = new JLabel("Enter the search term: ");
		JTextField textField = new JTextField("Type your search here");
		JPanel panel = new JPanel();
		termSearchButton = new JButton("Search");
		panel.add(text);
		panel.add(textField);

		panel.add(returnButton);
		returnButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){actionGUI();}});

		panel.add(termSearchButton);
		termSearchButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){searchTerm = textField.getText(); intoSearch();}});

		termSearchFrame.getContentPane().add(BorderLayout.CENTER, panel);
		actionFrame.setVisible(false);
		termSearchFrame.setVisible(true);		

	}

	private static void termSearchDisplayGUI()
	{

		termDisplayFrame = new JFrame("Jacob Musone's Search Engine");
		termDisplayFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		termDisplayFrame.setSize(500, 500);

		JLabel text = new JLabel("Search Results:");
		JPanel panel = new JPanel();
		panel.add(text);
		
		panel.add(returnButton);
		returnButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){actionGUI();}});

		//output here
		JTextArea result = new JTextArea(searchOutput);
		JScrollPane scrollBar = new JScrollPane(result);
		panel.add(result);
		panel.add(scrollBar);

		termDisplayFrame.getContentPane().add(BorderLayout.CENTER, panel);
		termSearchFrame.setVisible(false);
		termDisplayFrame.setVisible(true);	

	}

	private static void topNSearchGUI()
	{

		topNSearchFrame = new JFrame("Jacob Musone's Search Engine");
		topNSearchFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		topNSearchFrame.setSize(500, 500);

		JLabel text = new JLabel("Enter your N value: ");
		JTextField textField = new JTextField("Enter a N value");
		JPanel panel = new JPanel();
		topNSearchButton = new JButton("Search");
		panel.add(text);
		panel.add(textField);

		panel.add(returnButton);
		returnButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){actionGUI();}});

		panel.add(topNSearchButton);
		topNSearchButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){nVal = Integer.parseInt(textField.getText()); intoTopN()}});

		topNSearchFrame.getContentPane().add(BorderLayout.CENTER, panel);
		actionFrame.setVisible(false);
		topNSearchFrame.setVisible(true);				

	}

	private static void topNDisplayGUI()
	{

		topNDisplayFrame = new JFrame("Jacob Musone's Search Engine");
		topNDisplayFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		topNDisplayFrame.setSize(500, 500);

		JLabel text = new JLabel("Top-N Frequent Terms:");
		JPanel panel = new JPanel();
		panel.add(text);
		
		panel.add(returnButton);
		returnButton.addActionListener(new ActionListener(){public void actionPerformed(ActionEvent e){actionGUI();}});

		//output here
		JTextArea result = new JTextArea(topNOutput);
		JScrollPane scrollBar = new JScrollPane(result);
		panel.add(result);
		panel.add(scrollBar);

		topNDisplayFrame.getContentPane().add(BorderLayout.CENTER, panel);
		topNSearchFrame.setVisible(false);
		topNDisplayFrame.setVisible(true);	

	}

	private static void intoSearch()
	{

		//termSearchDisplayGUI()
		Scanner searcher = new Scanner(iiCompleteStorage);

		while(searcher.hasNext())
		{

			String line = scan.nextLine().toLowerCase().toString();
			
			if(line.contains(searchTerm))
			{

				searchOutput += "\n" + line;

			}

		}

		termSearchDisplayGUI();

	}

	private static void intoTopN()
	{

		try
		{
			JobPlacement tnJobPlace = JobPlacement.newBuilder().setClusterName(clusterName).build();
			tnJob = HadoopJob.newBuilder().setMainClass(tnMainClass).addJarFileUris("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/JAR/topN.jar").addArgs("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/iiOutput").addArgs("gs://dataproc-staging-us-central1-1057492895856-va2i33qh/tnOutput").addArgs(nVal).build();
			Job job = Job.newBuilder().setPlacement(tnJobPlace).setHadoopJob(tnJob).build();
			Job request = jobControllerClient.submitJob(projectID, region, job);
			String id = request.getReference().getJobId();
			CompletableFuture<Job> futureFinishedJob = CompletableFuture.supplyAsync(new Supplier<Job>(){@Override public Job get(){return waitForJobCompletion(jobControllerClient, projectID, region, id);}});
			
			int timeLimit = 10;

			try
			{

				Job jobInfo = futureFinishedJob.get(timeLimit, TimeUnit.MINUTES);
				System.out.println("Job " + id + "finished");

				Cluster cInfo = ccc.getCluster(projectID, region, clusterName);
				Storage tnStorage = StorageOptions.getDefaultInstance().getService();
				Blob blob = tnStorage.get(cInfo.getConfig().getConfigBucket(),"Output/part-r-00000");
				topNOutput += new String(blob.getContent());

			}
			catch(TimeoutException toEx)
			{

				System.out.println("Job timed out");

			}

		}
		catch(Exception e)
		{

			System.out.println("FAILURE");
			System.out.println(e);

			if(jcc != null)
			{

				jcc.close();
			}

		}

		topNDisplayGUI();

	}

}
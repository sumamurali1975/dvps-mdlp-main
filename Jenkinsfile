pipeline {
  agent any 

  environment {
    
    
    GITREPO             = "/var/lib/jenkins/workspace/${env.JOB_NAME}"
    DBTOKEN             = "databricks-token"
    CLUSTERID           = "0819-214501-chjkd9g9"
    DBURL               = "https://dbc-db420c65-4456.cloud.databricks.com"

    TESTRESULTPATH  ="${BUILDPATH}/Validation/reports/junit"
    LIBRARYPATH     = "${GITREPO}"
    OUTFILEPATH     = "${BUILDPATH}/Validation/Output"
    NOTEBOOKPATH    = "${GITREPO}"
    WORKSPACEPATH   = "/Demo-notebooks"               //"/Shared"
    DBFSPATH        = "dbfs:/FileStore/"
    BUILDPATH       = "${WORKSPACE}/Builds/${env.JOB_NAME}-${env.BUILD_NUMBER}"
    SCRIPTPATH      = "${GITREPO}/Scripts"
    projectName = "${WORKSPACE}"  
    projectKey = "key"
 }

  stages {
    stage('Install Miniconda') {
        steps {

            sh '''#!/usr/bin/env bash
            echo "Inicianddo os trabalhos"  
            wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -nv -O miniconda.sh
            rm -r $WORKSPACE/miniconda
            bash miniconda.sh -b -p $WORKSPACE/miniconda
            
            export PATH="$WORKSPACE/miniconda/bin:$PATH"
            echo $PATH
            conda config --set always_yes yes --set changeps1 no
            conda update -q conda
            conda create --name mlops2
            
            echo ${BUILDPATH}
	    
            '''
        }

    }

    stage('Install Requirements') {
        steps {
            sh '''#!/usr/bin/env bash
            echo "Installing Requirements"  
            source $WORKSPACE/miniconda/etc/profile.d/conda.sh
            
	    conda activate mlops2
            export PATH="$HOME/.local/bin:$PATH"
            echo $PATH
	    
	    # pip install --user databricks-cli
            # pip install -U databricks-connect
	    
            pip install -r requirements.txt
            databricks --version
           '''
        }

    }
	  
	stage('Databricks Setup') {
		steps{
			  withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
				sh """#!/bin/bash
				#Configure conda environment
				conda activate mlops2
				export PATH="$HOME/.local/bin:$PATH"
				echo $PATH
				# Configure Databricks CLI for deployment
				echo "${DBURL}
				$TOKEN" | databricks configure --token
				# Configure Databricks Connect for testing
				echo "${DBURL}
				$TOKEN
				${CLUSTERID}
				0
				15001" | databricks-connect configure
				
				"""
			  }	
		}
	}

	stage('Unit Tests') {
	      steps {

		script {
		    try {
			 withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {   
			      sh """#!/bin/bash
				export PYSPARK_PYTHON=/usr/local/bin/python3.8
				export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.8
				
				# Python tests
				pip install coverage-badge
				pip install coverage
				python3.8 -m pytest --junit-xml=${TESTRESULTPATH}/TEST-libout.xml ${LIBRARYPATH}/*/*_test*.py || true
				
				
				
				"""
			 }
		  } catch(err) {
		    step([$class: 'JUnitResultArchiver', testResults: '--junit-xml=${TESTRESULTPATH}/TEST-*.xml'])
		    if (currentBuild.result == 'UNSTABLE')
		      currentBuild.result = 'FAILURE'
		    throw err
		  }
		}
	      }
	    }
		
	stage('Build') {
		steps {
			
		    sh """mkdir -p "${BUILDPATH}/Workspace"
			  mkdir -p "${BUILDPATH}/Validation/Output"
			  
			  #Get Modified Files
			  git diff --name-only --diff-filter=AMR HEAD^1 HEAD | xargs -I '{}' cp --parents -r '{}' ${BUILDPATH}
			  
			  sudo rsync -av --exclude 'Builds' --exclude 'Jenkinsfile' --exclude 'miniconda' --exclude 'miniconda.sh' --exclude 'README.md' --exclude 'requirements.txt' --exclude 'XmlReport' --exclude '*_test.py' --exclude '.git' --exclude '.pytest_cache' --exclude '.scannerwork' --exclude '*.pyc' ${WORKSPACE}/  ${BUILDPATH}/Workspace/ 
			  rm -dr ${BUILDPATH}/Workspace/*/__pycache__
			  
			"""
			
		}

	}
	  
	stage('SonarQube Analysis') {
		  steps {
		    //def scannerhome = tool name: 'SonarQubeScanner'

		    withEnv(["PATH=/usr/bin:/usr/local/jdk-11.0.2/bin:/opt/sonarqube/sonar-scanner/bin/"]) {
			    withSonarQubeEnv('sonar') {
				    
				    sh """
				    source $WORKSPACE/miniconda/etc/profile.d/conda.sh
		     		    conda activate mlops2
				    """
				    
				    sh "/opt/sonar-scanner/bin/sonar-scanner -Dsonar.projectKey=MDLPPipeline -Dsonar.projectVersion=0.0.3 -Dsonar.sources=${BUILDPATH}/Workspace/ -Dsonar.host.url=http://107.20.71.233:9001 -Dsonar.login=ab9d8f9c15baff5428b9bf18b0ec198a5b35c6bb -Dsonar.python.xunit.reportPath=tests/unit/junit.xml -Dsonar.python.coverage.reportPath=${WORKSPACE}/coverage.xml -Dsonar.python.coveragePlugin=cobertura -Dsonar.sonar.inclusions=**/*.ipynb,**/*.py -Dsonar.exclusions=**/*.ini,**/*_test.py,**./*.sh"  
					
                                    sh ''' 
				       pip install coverage-badge
				       pip install coverage
		    		       pip install pytest-cov
				       rm -f  ${WORKSPACE}/*/addcol_test.py
				       #rm -f ${BUILDPATH}/Workspace/*/*.ipynb
		    		      #pytest --cov=${BUILDPATH}/Workspace/  --junitxml=./XmlReport/output.xml 
				       #python3 -m pytest --cov-report term --cov-report xml:coverage.xml --cov=${BUILDPATH}/Workspace/
				       python3 -m pytest --cov-report term --cov-report xml:coverage.xml --cov=${WORKSPACE}
                                       python -m coverage xml
				       #cp ${BUILDPATH}/Workspace/coverage.xml ${WORKSPACE}
				       				       
				       '''
				    
					 
			      }
		    }

		}
        }
   
	stage('Databricks Deploy') {
		 steps {
			withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
				sh """#!/bin/bash
				source $WORKSPACE/miniconda/etc/profile.d/conda.sh
				conda activate mlops2
				export PATH="$HOME/.local/bin:$PATH"
				# Use Databricks CLI to deploy notebooks
				databricks workspace mkdirs ${WORKSPACEPATH}
				databricks workspace import_dir --overwrite ${BUILDPATH}/Workspace/ ${WORKSPACEPATH}
				
				##databricks workspace import_dir --overwrite ${BUILDPATH}/Workspace/Framework ${WORKSPACEPATH}/Framework
				##databricks workspace import_dir --overwrite ${BUILDPATH}/Workspace/DataQuality ${WORKSPACEPATH}/DataQuality
				##databricks workspace import_dir --overwrite ${BUILDPATH}/Workspace/DataVault ${WORKSPACEPATH}/DataVault
				
				#dbfs cp -r ${BUILDPATH}/Libraries/python ${DBFSPATH}
				
				"""
				slackSend color: '#BADA55', message:'Pipeline Databricks Deploy Done'
				slackSend color: '#FF0000', message:' Databricks Pipeline Deployment Finished', iconEmoji: ":white_check_mark:"
		    	}
		 }
	}
	  
	stage('Report Test Results') {
		steps{
		  sh """find ${OUTFILEPATH} -name '*.json' -exec gzip --verbose {} \\;
			touch ${TESTRESULTPATH}/TEST-*.xml
		     """
		  junit "**/reports/junit/*.xml"
		}
	}
	  
  }
	
  post {
		success {
		  withAWS(credentials:'AWSCredentialsForSnsPublish') {
				snsPublish(
					topicArn:'arn:aws:sns:us-east-1:872161624847:mdlp-build-status-topic', 
					subject:"Job:${env.JOB_NAME}-Build Number:${env.BUILD_NUMBER} is a ${currentBuild.currentResult}", 
					message: "Please note that for Jenkins job:${env.JOB_NAME} of build number:${currentBuild.number} - ${currentBuild.currentResult} happened!"
				)
			}
		}
		failure {
		  withAWS(credentials:'AWSCredentialsForSnsPublish') {
				snsPublish(
					topicArn:'arn:aws:sns:us-east-1:872161624847:mdlp-build-status-topic', 
					subject:"Job:${env.JOB_NAME}-Build Number:${env.BUILD_NUMBER} is a ${currentBuild.currentResult}", 
					message: "Please note that for Jenkins job:${env.JOB_NAME} of build number:${currentBuild.number} - ${currentBuild.currentResult} happened! Details here: ${BUILD_URL}."
				)
			}
		}
  }
}

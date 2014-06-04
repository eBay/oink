/*
Copyright 2013-2014 eBay Software Foundation
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.pig.oink.common.service.impl;


import java.util.Properties;
import java.util.StringTokenizer;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.Logger;

public class EmailNotification implements Runnable {

	private String fromEmail;
	private String toEmail;
	private String smtpHost;
	private String subject;
	private String body;
	private static final Logger logger = Logger.getLogger(EmailNotification.class);
	
	public EmailNotification(String fromEmail, String toEmail, String smtpHost, String subject, String body) {
		this.fromEmail = fromEmail;
		this.toEmail = toEmail;
		this.smtpHost = smtpHost;
		this.subject = subject;
		this.body = body;
	}
	
	@Override
	public void run() {
		Properties properties = System.getProperties();
		properties.setProperty("mail.host", smtpHost);
		properties.setProperty("mail.smtp.host", smtpHost);
		try{
			Session session = Session.getDefaultInstance(properties, null);
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(fromEmail));

			// Set To: header field of the header.
			StringTokenizer stk = new StringTokenizer(toEmail, ",");
			InternetAddress[] toList = new InternetAddress[stk.countTokens()];
			int count = 0;
			while(stk.hasMoreTokens()) {
				String token = stk.nextToken().trim();
				toList[count++] = new InternetAddress(token);
			}
			message.setRecipients(Message.RecipientType.TO, toList);

			// Set Subject: header field
			message.setSubject(subject);

			// Now set the actual message
			message.setContent(body, "text/plain");
			// Send message
			Transport.send(message);
			logger.info("Email sent successfully to " + toEmail);
		} catch (MessagingException mex) {
			logger.error("Error in sending email", mex);
		} catch(Exception e) {
			logger.error("Error in sending email", e);
		}
	}
}

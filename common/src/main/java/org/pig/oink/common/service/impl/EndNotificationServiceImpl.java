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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.pig.oink.common.service.EndNotificationService;

public final class EndNotificationServiceImpl implements EndNotificationService {
	private ExecutorService emailExecutors;
	private ExecutorService httpcallbackExecutors;
	public static EndNotificationService endNotificationService = new EndNotificationServiceImpl();
	
	private EndNotificationServiceImpl() {
		emailExecutors = Executors.newCachedThreadPool();
		httpcallbackExecutors = Executors.newCachedThreadPool();
	}
	
	public static EndNotificationService getService() {
		return endNotificationService;
	}
	
	@Override
	public void sendEmail(String fromEmail, String toEmail, String smtpHost, String subject, String body) {
		emailExecutors.submit(new EmailNotification(fromEmail, toEmail, smtpHost, subject, body));
	}

	@Override
	public void sendHttpNotification(String url, Map<String, String> parameters) {
		for(Map.Entry<String, String> entry : parameters.entrySet()) {
			url = url.replace(entry.getKey(), entry.getValue());
		}
		httpcallbackExecutors.submit(new HttpNotification(url));
	}

}

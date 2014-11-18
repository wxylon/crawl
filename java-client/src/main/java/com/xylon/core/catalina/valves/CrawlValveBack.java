package com.xylon.core.catalina.valves;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class CrawlValveBack extends ValveBase{
	
	protected final Log log = LogFactory.getLog(getClass());
	
	private String _requestUriIgnorePattern;
	private Pattern _ignorePattern = null;
	private static final AtomicBoolean _enabled = new AtomicBoolean(true);
	private static final String IP = "X-Real-IP";
	private static final String Referer = "Referer";
	private static final String UA = "User-Agent";
	
	@Override
	public void initInternal() throws LifecycleException{
		super.initInternal();
		if (_requestUriIgnorePattern != null) {
			this._ignorePattern = Pattern.compile(_requestUriIgnorePattern);
		} else {
			this._ignorePattern = null;
		}
	}

	public void invoke(Request request, Response response) throws IOException, ServletException {
		if ( !_enabled.get() || _ignorePattern != null && _ignorePattern.matcher(request.getRequestURI()).matches() ) {
            getNext().invoke( request, response );
		} else {
			//sedis.get(key);
			log.info(getHeaderValue(request, IP));
			log.info(getHeaderValue(request, Referer));
			log.info(request.getRemoteAddr());
			log.info(getHeaderValue(request, UA));
			getNext().invoke( request, response );
		}
	}
	
	private String getHeaderValue(Request request, String name){
		String reqValue = request.getHeader(name);
		return reqValue;
	}
	
	private String getCookieValue(Request request, String key) {
		String sCookie = request.getHeader("cookie");
		if (sCookie == null) {
			return null;
		}
		String[] cookies = sCookie.split("; ");
		for (String s : cookies) {
			String[] parts = s.split("=", 2);
			if (parts.length == 2) {
				String cname = parts[0];
				if (cname.equals(key)) {
					String cval = parts[1];
					return converted(cval);
				}
			}
		}

		return null;
	}
	
	private String converted(String s) {
		if (s.indexOf("\"") == 0 && s.length() >= 2) {
			s = s.substring(1, s.length() - 1);
		}
		//截断到第一个分号
		//UC某些cookie有问题
		if(s.indexOf(";") != -1){
			s = s.substring(0,s.indexOf(";"));
		}
		
		return s;
	}

	public void setRequestUriIgnorePattern(String requestUriIgnorePattern) {
		this._requestUriIgnorePattern = requestUriIgnorePattern;
	}
}


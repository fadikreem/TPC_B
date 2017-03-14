package tpc.b;
/*
Copyright (c) 2010, Radim Kolar
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.
*/

import java.sql.SQLException;
import java.util.Random;

public class worker extends Thread {

	protected bench b;
	protected long stoptime;
	protected int transactions;

	public worker(ThreadGroup group, bench b) {
		super(group,"worker");
		this.b=b;
		this.stoptime = Long.MAX_VALUE;
		this.transactions = 10000;
	}

	public void setStopTime(long stop) {
		if ( stop == 0L )
			stoptime = Long.MAX_VALUE;
		else
			stoptime = stop;
	}

	public void setTranscations(int tmax) {
		if (tmax == 0)
			transactions = Integer.MAX_VALUE;
		else
			transactions = tmax;
	}
	
	public bench getBench() {
		return b;
	}
	
	@Override
	public void run() {
		SUT sut = null;
		long start;
		int counter;
		try {
			sut = b.getDriver().getNewSUT();
			sut.init(b.getSchema());
		} catch (SQLException e) {
			sut.dispose();
			return;
		}
		start = System.currentTimeMillis();
		int scale = b.getScaleFactor();
		Random rand = new Random();
		counter = 0;


		while(counter++ < transactions) {
			long now;
			boolean failed = false;
			try {
				int aid,bid;
				bid = rand.nextInt(scale);
				if (rand.nextFloat() < 0.85f )
					aid = rand.nextInt(100000) + bid*100000 + 1;
				else
					aid = rand.nextInt(100000*scale)+1;
				sut.executeProfile(bid+1, rand.nextInt(10)+bid*10+1, aid, rand.nextInt(999999*2)-999999);
				sut.commit();
			}
			catch (SQLException err) {
				failed = true;
				try {
					sut.rollback();
				} catch (Exception ignored ) {}
			}

			now = System.currentTimeMillis();
			if ( now > stoptime)
				break;
			if ( ! failed )
				b.reportTransaction(now - start);
			else
				b.reportFailedTransaction();
			start = now;
		}
		sut.dispose();
	}
}

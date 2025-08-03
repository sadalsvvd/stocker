import { EODHistoricalService } from './src/services/eodHistorical.ts';

// Quick test of the EOD service
async function testFetch() {
  try {
    const service = new EODHistoricalService();
    console.log('Fetching AAPL data for last 5 days...');
    
    const endDate = new Date().toISOString().split('T')[0];
    const startDate = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    const data = await service.fetchDaily('AAPL', startDate, endDate);
    console.log(`Fetched ${data.length} records:`);
    data.forEach(d => {
      console.log(`${d.date}: Open=${d.open}, Close=${d.close}, Volume=${d.volume}`);
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run if this file is executed directly
if (import.meta.main) {
  testFetch();
}
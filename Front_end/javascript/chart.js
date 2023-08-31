chartIt();

async function chartIt(){
    const ausData = await getData();
    const ctx = document.getElementById('chart1');
    new Chart(ctx, {
    type: 'bar',
    data: {
        labels: ausData.xs,
        datasets: [{
        label: 'electricity',
        data: ausData.ys,
        borderWidth: 1
        }]
    },
    options: {
        scales: {
        y: {
            beginAtZero: true
        }
        }
    }
    });}

  async function getData(){
    const xs=[];
    const ys=[];

    return {xs,ys};
  }
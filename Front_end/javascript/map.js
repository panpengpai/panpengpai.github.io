document.querySelectorAll(".paths").forEach((path) => {
  path.addEventListener("mouseover", function (e) {
    const divElement = document.getElementById("aus-map"); // Replace with your div's ID
    const rect = divElement.getBoundingClientRect();
    
    x = e.clientX - rect.left; // Calculate x relative to the div
    y = e.clientY - rect.top; // Calculate y relative to the div
    
    document.getElementById("map-tip").style.top = y - 120 + "px";
    document.getElementById("map-tip").style.left = x - 120 + "px";

    document.getElementById("state-name").innerHTML = path.id;
    document.getElementById("map-tip").style.opacity = 0.7;
  });

  path.addEventListener("mouseleave", function () {
    document.getElementById("map-tip").style.opacity = 0;
  });
});


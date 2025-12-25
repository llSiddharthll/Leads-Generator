let currentPage = 1;
const pageSize = 10;
let data = [];

document.getElementById("searchForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    currentPage = 1;

    const niche = document.getElementById("niche").value;
    const location = document.getElementById("location").value;
    const radius = document.getElementById("radius").value;

    const response = await fetch(
        `/find-businesses?niche=${niche}&location=${location}&radius_km=${radius}`
    );

    const result = await response.json();
    data = result.businesses || [];

    renderTable();
});

document.getElementById("prev").onclick = () => {
    if (currentPage > 1) {
        currentPage--;
        renderTable();
    }
};

document.getElementById("next").onclick = () => {
    if (currentPage * pageSize < data.length) {
        currentPage++;
        renderTable();
    }
};

function renderTable() {
    const tbody = document.querySelector("#resultsTable tbody");
    tbody.innerHTML = "";

    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;

    data.slice(start, end).forEach(b => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${b.name || "-"}</td>
            <td>${b.category || "-"}</td>
            <td>${b.contact?.phone || "-"}</td>
            <td>
              ${b.contact?.website 
                ? `<a href="${b.contact.website}" target="_blank">Visit</a>` 
                : "-"}
            </td>
            <td>${b.address?.city || "-"}</td>
        `;
        tbody.appendChild(row);
    });

    document.getElementById("pageInfo").innerText =
        `Page ${currentPage} of ${Math.ceil(data.length / pageSize)}`;
}

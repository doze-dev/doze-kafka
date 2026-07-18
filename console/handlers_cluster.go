package console

import "net/http"

func (c *Console) cluster(w http.ResponseWriter, r *http.Request) {
	info, err := c.be.ClusterInfo(r.Context())
	if err != nil {
		c.fail(w, err)
		return
	}
	c.render(w, r, "cluster_detail", map[string]any{"Cluster": info, "Page": "cluster", "Title": "Cluster"})
}

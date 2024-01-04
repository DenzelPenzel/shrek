package dyport

import (
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func TestDyPort(t *testing.T) {
	t.Run("test success allocation", func(t *testing.T) {
		count := 5
		ports, err := AllocatePorts(count)
		require.NoError(t, err)
		require.Equal(t, count, len(ports))

		for _, port := range ports {
			ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
			require.NoError(t, err)
			ln.Close()
		}
	})
}

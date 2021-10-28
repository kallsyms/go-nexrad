package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/g3n/engine/app"
	"github.com/g3n/engine/camera"
	"github.com/g3n/engine/core"
	"github.com/g3n/engine/geometry"
	"github.com/g3n/engine/gls"
	"github.com/g3n/engine/graphic"
	"github.com/g3n/engine/gui"
	"github.com/g3n/engine/light"
	"github.com/g3n/engine/material"
	"github.com/g3n/engine/math32"
	"github.com/g3n/engine/renderer"
	"github.com/g3n/engine/util/helper"
	"github.com/g3n/engine/window"
	"github.com/kallsyms/go-nexrad/archive2"
)

func main() {

	// Create application and scene
	a := app.App()
	scene := core.NewNode()

	// Set the scene to be managed by the gui manager
	gui.Manager().Set(scene)

	// Create perspective camera
	cam := camera.New(1)
	cam.SetPosition(0, 0, 10)
	scene.Add(cam)

	// Set up orbit control for the camera
	control := camera.NewOrbitControl(cam)
	control.MinDistance = 0.5
	control.SetEnabled(camera.OrbitZoom | camera.OrbitPan | camera.OrbitKeys)

	// Set up callback to update viewport and camera aspect ratio when the window is resized
	onResize := func(evname string, ev interface{}) {
		// Get framebuffer size and update viewport accordingly
		width, height := a.GetSize()
		a.Gls().Viewport(0, 0, int32(width), int32(height))
		// Update the camera's aspect ratio
		cam.SetAspect(float32(width) / float32(height))
	}
	a.Subscribe(window.OnWindowSize, onResize)
	onResize("", nil)

	var ar2 *archive2.Archive2 = archive2.NewArchive2("/home/nickgregory/dev/weather_maps/l2rad/KOKX20210902_000428_V06")

	// load the nexrad file
	elevAngle := 0
	elevAngles := make([]float32, 0)
	for _, msgs := range ar2.ElevationScans {
		elevAngles = append(elevAngles, msgs[elevAngle].Header.ElevationAngle)
	}
	sort.Slice(elevAngles, func(left, right int) bool {
		return elevAngles[left] < elevAngles[right]
	})

	// dropdown for elevation control
	elevDropdown := gui.NewDropDown(150, gui.NewImageLabel("Select Elevation"))
	elevDropdown.SetPosition(10, 20)
	scene.Add(elevDropdown)

	for _, angle := range elevAngles {
		elevDropdown.Add(
			gui.NewImageLabel(fmt.Sprintf("%.1f", angle)),
		)
	}

	// load in the goods to the scene
	geom := LoadNEXRAD(ar2.ElevationScans[elevAngle], scene)

	elevDropdown.Subscribe(gui.OnChange, func(eventName string, event interface{}) {
		// fmt.Println(eventName, elevDropdown.SelectedPos(), elevDropdown.Selected().Text())
		elevAngle = elevDropdown.SelectedPos()
		geom.Dispose()
		geom = LoadNEXRAD(ar2.ElevationScans[elevAngle], scene)
	})

	// TODO profiling shows the VBO is holding on to the giant float arrays after transfer
	// f, _ := os.Create("out.prof")
	// pprof.WriteHeapProfile(f)

	// Create and add lights to the scene
	scene.Add(light.NewAmbient(&math32.Color{1.0, 1.0, 1.0}, 0.8))
	pointLight := light.NewPoint(&math32.Color{1, 1, 1}, 5.0)
	pointLight.SetPosition(1, 0, 2)
	scene.Add(pointLight)

	// custom shader
	a.Renderer().AddShader("flatVert", vertexShader)
	a.Renderer().AddShader("flatFrag", fragmentShader)
	a.Renderer().AddProgram("flatShader", "flatVert", "flatFrag", "")

	// Create and add an axis helper to the scene
	scene.Add(helper.NewAxes(0.5))

	// Set background color to gray
	a.Gls().ClearColor(0.71, 0.71, 0.71, 1.0)

	// Run the application
	a.Run(func(renderer *renderer.Renderer, deltaTime time.Duration) {
		a.Gls().Clear(gls.DEPTH_BUFFER_BIT | gls.STENCIL_BUFFER_BIT | gls.COLOR_BUFFER_BIT)
		renderer.Render(scene, cam)
	})
}

func LoadNEXRAD(msgs []*archive2.Message31, scene *core.Node) *geometry.Geometry {

	positions := math32.NewArrayF32(0, 0)
	indices := math32.NewArrayU32(0, 0)
	colors := math32.NewArrayF32(0, 0)

	idx := uint32(0)
	startAngle := float32(0)
	endAngle := float32(0)

	for i, msg := range msgs {

		// get the start/end angles for this radial
		if i == 0 {
			startAngle = msg.Header.AzimuthAngle
		} else {
			startAngle = endAngle
		}

		if startAngle < 0 {
			startAngle += 360
		}

		endAngle = startAngle + 0.5
		if endAngle > 360 {
			endAngle -= 360
		}

		startRadian := startAngle * (math32.Pi / 180)
		endRadian := endAngle * (math32.Pi / 180)

		// convert to cartesian relative to the radar center
		rangeKM := float32(msg.REFData.DataMomentRange) / 1000.
		gateHeightKM := float32(msg.REFData.DataMomentRangeSampleInterval) / 1000.

		data := msg.REFData.ScaledData()

		lyComponent := math32.Cos(startRadian)
		lxComponent := math32.Sin(startRadian)
		uyComponent := math32.Cos(endRadian)
		uxComponent := math32.Sin(endRadian)

		innerRange := rangeKM
		outerRange := innerRange + gateHeightKM

		for gate := 0; gate < int(msg.REFData.NumberDataMomentGates); gate++ {

			// add our four corners
			positions.Append(innerRange*lxComponent, innerRange*lyComponent, 0)
			positions.Append(innerRange*uxComponent, innerRange*uyComponent, 0)
			positions.Append(outerRange*uxComponent, outerRange*uyComponent, 0)
			positions.Append(outerRange*lxComponent, outerRange*lyComponent, 0)

			// and these indices create the two triangles
			indices.Append(idx, idx+1, idx+2, idx, idx+2, idx+3)
			idx += 4

			// add the color data, which is the same for each vertex
			refl := data[gate]
			color := cmap(refl)
			colors.AppendColor4(&color)
			colors.AppendColor4(&color)
			colors.AppendColor4(&color)
			colors.AppendColor4(&color)

			innerRange = outerRange
			outerRange += gateHeightKM
		}
	}

	geom := geometry.NewGeometry()
	geom.AddVBO(gls.NewVBO(positions).AddAttrib(gls.VertexPosition))
	geom.AddVBO(gls.NewVBO(colors).AddCustomAttrib("VertexColor4", 4))
	geom.SetIndices(indices)

	mat := material.NewBasic()
	mat.SetShader("flatShader")

	mesh := graphic.NewMesh(geom, mat)
	scene.Add(mesh)

	// this doesnt really seem to matter
	positions = nil
	indices = nil
	colors = nil

	return geom
}

// color map thing
func cmap(val float32) math32.Color4 {
	c := math32.Color4{R: 0, G: 0, B: 0, A: 0}
	switch v := val; {
	case v == 999:
		c = math32.Color4{R: 0, G: 0, B: 0, A: 0}
	case v >= 90:
		c = math32.Color4{R: 0, G: 0, B: 255, A: 255}
	case v >= 85:
		c = math32.Color4{R: 0, G: 0, B: 0, A: 255}
	case v >= 80:
		c = math32.Color4{R: 128, G: 128, B: 128, A: 255}
	case v >= 75:
		c = math32.Color4{R: 255, G: 255, B: 255, A: 255}
	case v >= 70:
		c = math32.Color4{R: 125, G: 0, B: 255, A: 255}
	case v >= 65:
		c = math32.Color4{R: 255, G: 0, B: 255, A: 255}
	case v >= 60:
		c = math32.Color4{R: 200, G: 15, B: 175, A: 255}
	case v >= 55:
		c = math32.Color4{R: 175, G: 0, B: 75, A: 255}
	case v >= 50:
		c = math32.Color4{R: 255, G: 0, B: 50, A: 255}
	case v >= 45:
		c = math32.Color4{R: 255, G: 75, B: 0, A: 255}
	case v >= 40:
		c = math32.Color4{R: 255, G: 150, B: 0, A: 255}
	case v >= 35:
		c = math32.Color4{R: 255, G: 255, B: 0, A: 255}
	case v >= 30:
		c = math32.Color4{R: 220, G: 220, B: 0, A: 255}
	case v >= 25:
		c = math32.Color4{R: 60, G: 220, B: 20, A: 255}
	}

	return *c.MultiplyScalar(1 / 255.)
}

const vertexShader = `
in vec3 VertexPosition;
in vec4 VertexColor4;

// Model uniforms
uniform mat4 MVP;

// Final output color for fragment shader
out vec4 Color;

void main() {

    Color = VertexColor4;
    gl_Position = MVP * vec4(VertexPosition, 1.0);
}
`

const fragmentShader = `
precision highp float;

in vec4 Color;
out vec4 FragColor;

void main() {

    FragColor = Color;
}
`

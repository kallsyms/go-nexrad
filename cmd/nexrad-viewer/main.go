package main

import (
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
	"github.com/jddeal/go-nexrad/archive2"
)

func main() {

	// Create application and scene
	a := app.App()
	scene := core.NewNode()

	// Set the scene to be managed by the gui manager
	gui.Manager().Set(scene)

	// Create perspective camera
	cam := camera.New(1)
	cam.SetPosition(0, 0, 3)
	scene.Add(cam)

	// Set up orbit control for the camera
	control := camera.NewOrbitControl(cam)
	control.MinDistance = 0.25

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

	// load in the goods to the scene
	LoadNEXRAD(scene)

	// Create and add lights to the scene
	scene.Add(light.NewAmbient(&math32.Color{1.0, 1.0, 1.0}, 0.8))
	pointLight := light.NewPoint(&math32.Color{1, 1, 1}, 5.0)
	pointLight.SetPosition(1, 0, 2)
	scene.Add(pointLight)

	// Create and add an axis helper to the scene
	scene.Add(helper.NewAxes(0.5))

	// Set background color to gray
	a.Gls().ClearColor(0.1, 0.1, 0.1, 1.0)

	// custom shader
	a.Renderer().AddShader("flatVert", vertexShader)
	a.Renderer().AddShader("flatFrag", fragmentShader)
	a.Renderer().AddProgram("flatShader", "flatVert", "flatFrag", "")

	// Run the application
	a.Run(func(renderer *renderer.Renderer, deltaTime time.Duration) {
		a.Gls().Clear(gls.DEPTH_BUFFER_BIT | gls.STENCIL_BUFFER_BIT | gls.COLOR_BUFFER_BIT)
		renderer.Render(scene, cam)
	})
}

func LoadNEXRAD(scene *core.Node) {

	// load the nexrad data
	ar2 := archive2.NewArchive2("./KMPX20200906_053126_V06")

	// grab the lowest elevation scan's messages
	msgs := ar2.ElevationScans[1]

	for _, msg := range msgs {

		// get the start/end angles for this radial
		// azimuth := math32.Round(msg.Header.AzimuthAngle/0.5) * 0.5
		azimuth := msg.Header.AzimuthAngle
		startAngle := azimuth
		if startAngle < 0 {
			startAngle += 360
		}

		gateWidthDegrees := msg.AzimuthResolutionSpacing()
		endAngle := azimuth + gateWidthDegrees
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

		positions := math32.NewArrayF32(0, 0)
		indices := math32.NewArrayU32(0, 0)
		colors := math32.NewArrayF32(0, 0)

		innerRange := rangeKM + (float32(0) * gateHeightKM)
		outerRange := innerRange + gateHeightKM
		lly := innerRange * lyComponent
		llx := innerRange * lxComponent
		lry := innerRange * uyComponent
		lrx := innerRange * uxComponent

		uly := outerRange * lyComponent
		ulx := outerRange * lxComponent
		ury := outerRange * uyComponent
		urx := outerRange * uxComponent

		// log.Printf("msg %03d: θ=%3.1f to %3.1f", i, startAngle, endAngle)

		for gate := 0; gate < int(msg.REFData.NumberDataMomentGates); gate++ {

			refl := data[gate]
			color := cmap(refl)
			color3 := color.ToColor()

			// log.Printf("  msg %03d: θ=%3.1f to %3.1f is %2.1f dBZ which maps to %v", i, startAngle, endAngle, refl, color)

			// first rectangle handled separately
			if gate == 0 {
				positions.Append(llx, lly, 0)
				positions.Append(lrx, lry, 0)
				positions.Append(urx, ury, 0)
				positions.Append(ulx, uly, 0)
				indices.Append(0, 1, 2, 0, 2, 3)
				colors.AppendColor(&color3)
				colors.AppendColor(&color3)
				colors.AppendColor(&color3)
				colors.AppendColor(&color3)
				continue
			}

			idx := (uint32(gate) * 2) + 1

			outerRange += 0.1
			uly := outerRange * lyComponent
			ulx := outerRange * lxComponent
			ury := outerRange * uyComponent
			urx := outerRange * uxComponent

			positions.Append(urx, ury, 0)
			positions.Append(ulx, uly, 0)
			indices.Append(idx, idx-1, idx+1, idx, idx+1, idx+2)

			colors.AppendColor(&color3)
			colors.AppendColor(&color3)
		}

		geom := geometry.NewGeometry()
		geom.SetIndices(indices)
		geom.AddVBO(gls.NewVBO(positions).AddAttrib(gls.VertexPosition))
		geom.AddVBO(gls.NewVBO(colors).AddAttrib(gls.VertexColor))

		mat := material.NewBasic()
		mat.SetShader("flatShader")

		mesh := graphic.NewMesh(geom, mat)
		scene.Add(mesh)

		positions = nil
		indices = nil
		colors = nil
	}
}

// color map thing
func cmap(val float32) math32.Color4 {
	c := math32.Color4{0, 0, 0, 0}
	switch v := val; {
	case v == 999:
		c = math32.Color4{0, 0, 0, 0}
	case v >= 90:
		c = math32.Color4{0, 0, 255, 1}
	case v >= 85:
		c = math32.Color4{0, 0, 0, 1}
	case v >= 80:
		c = math32.Color4{128, 128, 128, 1}
	case v >= 75:
		c = math32.Color4{255, 255, 255, 1}
	case v >= 70:
		c = math32.Color4{125, 0, 255, 1}
	case v >= 65:
		c = math32.Color4{255, 0, 255, 1}
	case v >= 60:
		c = math32.Color4{200, 15, 175, 1}
	case v >= 55:
		c = math32.Color4{175, 0, 75, 1}
	case v >= 50:
		c = math32.Color4{255, 0, 50, 1}
	case v >= 45:
		c = math32.Color4{255, 75, 0, 1}
	case v >= 40:
		c = math32.Color4{255, 150, 0, 1}
	case v >= 35:
		c = math32.Color4{255, 255, 0, 1}
	case v >= 30:
		c = math32.Color4{220, 220, 0, 1}
	case v >= 25:
		c = math32.Color4{60, 220, 20, 1}
	}

	return math32.Color4{c.R / 255, c.G / 255, c.B / 255, c.A / 255}
}

const vertexShader = `
#include <attributes>

// Model uniforms
uniform mat4 MVP;

// Final output color for fragment shader
flat out vec3 Color;

void main() {

    Color = VertexColor;
    gl_Position = MVP * vec4(VertexPosition, 1.0);
}
`

const fragmentShader = `
precision highp float;

flat in vec3 Color;
out vec4 FragColor;

void main() {
    FragColor = vec4(Color, 1.0);
}
`

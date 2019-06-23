package com.hatran;

import java.awt.*;

//实例空间位置
public class PositionInSpace {
    private double X;
    private double Y;
    private int Feature;
    private int Instance;

    public void setX(double X) {
        this.X = X;
    }

    public double getX() {
        return this.X;
    }

    public void setY(double Y) {
        this.Y = Y;
    }

    public double getY() {
        return this.Y;
    }

    public void setFeature(int Feature) {
        this.Feature = Feature;
    }

    public int getFeature() {
        return this.Feature;
    }

    public void setInstance(int Instance) {
        this.Instance = Instance;
    }

    public int getInstance() {
        return this.Instance;
    }

    public void DisplayPositionInSpace() {
        System.out.print("(" + X + "," + Y + ")");
    }

    @Override //
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;

        if (obj == this)
            return true;

        PositionInSpace pos = (PositionInSpace) obj;
        return Feature == pos.Feature && Instance == pos.Instance;

    }

    @Override
    public int hashCode() {
        return Feature;
    }

}